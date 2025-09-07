from django.shortcuts import render, get_object_or_404, redirect
from django.urls import reverse_lazy, reverse
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from alm_app.models import BehavioralPattern, BehavioralPatternSplit
from django import forms
from django.forms import inlineformset_factory, BaseInlineFormSet
from django.contrib import messages
from alm_app.models import Stg_Product_Master
import logging
from django.http import JsonResponse, HttpResponseNotAllowed, HttpResponseRedirect
from urllib.parse import quote_plus

# Set up logging
logger = logging.getLogger(__name__)

# Forms
class BehavioralPatternForm(forms.ModelForm):
    v_prod_type = forms.ChoiceField(required=True)

    class Meta:
        model = BehavioralPattern
        fields = ['v_prod_type', 'description']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Build choices from distinct product types in Stg_Product_Master
        distinct_types = (
            Stg_Product_Master.objects
            .exclude(v_prod_type__isnull=True)
            .exclude(v_prod_type__exact='')
            .values_list('v_prod_type', flat=True)
            .distinct()
            .order_by('v_prod_type')
        )
        choices = [(pt, pt) for pt in distinct_types]
        self.fields['v_prod_type'].choices = [('', 'Select a product type')] + choices
        # If editing an existing pattern, ensure current value is present even if not in staging anymore
        if self.instance and self.instance.pk and self.instance.v_prod_type:
            if self.instance.v_prod_type not in dict(choices):
                self.fields['v_prod_type'].choices += [
                    (self.instance.v_prod_type, f"{self.instance.v_prod_type} (current)")
                ]

    def _post_clean(self):
        # For new instances (no PK), temporarily disable model.clean() to avoid touching related managers
        if not self.instance.pk:
            original_clean = getattr(self.instance, 'clean', None)
            try:
                self.instance.clean = lambda: None
                super()._post_clean()
            finally:
                if original_clean is not None:
                    self.instance.clean = original_clean
        else:
            super()._post_clean()

class BehavioralPatternSplitForm(forms.ModelForm):
    class Meta:
        model = BehavioralPatternSplit
        fields = ['bucket_number', 'percentage']

    def clean(self):
        cleaned = super().clean()
        # If this row is marked for deletion by the formset, skip further validation
        if self.cleaned_data.get('DELETE'):
            return cleaned
        bucket = cleaned.get('bucket_number')
        pct = cleaned.get('percentage')
        if bucket is None:
            self.add_error('bucket_number', 'This field is required.')
        if pct is None:
            self.add_error('percentage', 'This field is required.')
        return cleaned

    def _post_clean(self):
        # Skip model-level clean when row is being deleted or required values are missing
        skip_model_clean = (
            self.cleaned_data.get('DELETE') or
            self.cleaned_data.get('bucket_number') is None or
            self.cleaned_data.get('percentage') is None
        )
        if skip_model_clean:
            original_clean = getattr(self.instance, 'clean', None)
            try:
                self.instance.clean = lambda: None
                super()._post_clean()
            finally:
                if original_clean is not None:
                    self.instance.clean = original_clean
        else:
            super()._post_clean()

class SplitInlineFormSet(BaseInlineFormSet):
    def __init__(self, *args, **kwargs):
        # If instance is provided but doesn't have a PK, don't pass it to avoid relationship errors
        if 'instance' in kwargs and kwargs['instance'] and not kwargs['instance'].pk:
            logger.info("Removing instance from formset kwargs to avoid relationship error")
            del kwargs['instance']
        super().__init__(*args, **kwargs)

    def clean(self):
        logger.info(f"SplitInlineFormSet.clean() called. Instance: {self.instance}, PK: {getattr(self.instance, 'pk', 'None')}")
        # Skip all validation if the parent instance doesn't have a primary key yet
        if not hasattr(self, 'instance') or not self.instance or not self.instance.pk:
            logger.info("Skipping formset validation - no instance or no PK")
            return
            
        super().clean()
        total = 0
        seen_buckets = set()
        for form in self.forms:
            if getattr(form, 'cleaned_data', None) and not form.cleaned_data.get('DELETE', False):
                bucket = form.cleaned_data.get('bucket_number')
                pct = form.cleaned_data.get('percentage') or 0
                if bucket in seen_buckets:
                    form.add_error('bucket_number', 'Duplicate bucket number')
                seen_buckets.add(bucket)
                total += pct
        if total > 100:
            raise forms.ValidationError('Total percentage across all buckets must not exceed 100%.')

    def is_valid(self):
        logger.info(f"SplitInlineFormSet.is_valid() called. Instance: {self.instance}, PK: {getattr(self.instance, 'pk', 'None')}")
        # Skip validation if the parent instance doesn't have a primary key yet
        if not hasattr(self, 'instance') or not self.instance or not self.instance.pk:
            logger.info("Skipping formset validation - no instance or no PK")
            return True
        return super().is_valid()

SplitFormSet = inlineformset_factory(
    parent_model=BehavioralPattern,
    model=BehavioralPatternSplit,
    form=BehavioralPatternSplitForm,
    formset=SplitInlineFormSet,
    fields=['bucket_number', 'percentage'],
    extra=1,
    can_delete=True
)

# BehavioralPattern Views
class BehavioralPatternListView(ListView):
    model = BehavioralPattern
    template_name = 'behavioural/pattern_list.html'
    context_object_name = 'patterns'

class BehavioralPatternCreateView(CreateView):
    model = BehavioralPattern
    form_class = BehavioralPatternForm
    template_name = 'behavioural/pattern_form.html'
    success_url = reverse_lazy('alm_app:pattern-list')

class BehavioralPatternUpdateView(UpdateView):
    model = BehavioralPattern
    form_class = BehavioralPatternForm
    template_name = 'behavioural/pattern_form.html'
    success_url = reverse_lazy('alm_app:pattern-list')

class BehavioralPatternDeleteView(DeleteView):
    model = BehavioralPattern
    template_name = 'behavioural/pattern_confirm_delete.html'
    success_url = reverse_lazy('alm_app:pattern-list')

    def delete(self, request, *args, **kwargs):
        self.object = self.get_object()
        v_prod_type = self.object.v_prod_type
        response = super().delete(request, *args, **kwargs)
        messages.success(request, f'Pattern "{v_prod_type}" deleted successfully.')
        return response

# Combined manage view (product + its bucket percentages)
def pattern_manage(request, pk=None):
    logger.info(f"pattern_manage called with pk={pk}, method={request.method}")
    
    if pk:
        pattern = get_object_or_404(BehavioralPattern, pk=pk)
        logger.info(f"Editing existing pattern: {pattern.pk}")
    else:
        pattern = BehavioralPattern()
        logger.info("Creating new pattern")

    if request.method == 'POST':
        logger.info("Processing POST request")
        
        try:
            logger.info("Creating BehavioralPatternForm")
            form = BehavioralPatternForm(request.POST, instance=pattern)
            
            logger.info("Validating form")
            if form.is_valid():
                logger.info("Form is valid, saving pattern")
                # Save the pattern first to get a primary key
                pattern = form.save()
                logger.info(f"Pattern saved with PK: {pattern.pk}")
                
                # Now create formset with the saved pattern instance
                logger.info("Creating formset with saved pattern")
                formset = SplitFormSet(request.POST, instance=pattern)
                
                logger.info("Validating formset")
                if formset.is_valid():
                    logger.info("Formset is valid, saving")
                    formset.save()
                    logger.info("Formset saved successfully")
                    messages.success(request, 'Pattern and bucket splits saved successfully.')
                    return redirect('alm_app:pattern-list')
                else:
                    logger.error(f"Formset validation failed: {formset.errors}")
                    messages.error(request, 'Please correct the bucket split errors below.')
            else:
                logger.error(f"Form validation failed: {form.errors}")
                messages.error(request, 'Please correct the product errors below.')
                # For invalid form, create empty formset to avoid errors
                if pattern.pk:
                    logger.info("Creating formset for existing pattern")
                    formset = SplitFormSet(instance=pattern)
                else:
                    logger.info("Creating empty formset for new pattern")
                    formset = SplitFormSet()
        except Exception as e:
            logger.error(f"Exception in pattern_manage: {str(e)}", exc_info=True)
            messages.error(request, f'An error occurred: {str(e)}')
            # Create forms for display
            form = BehavioralPatternForm(request.POST, instance=pattern)
            if pattern.pk:
                formset = SplitFormSet(instance=pattern)
            else:
                formset = SplitFormSet()
    else:
        logger.info("Processing GET request")
        form = BehavioralPatternForm(instance=pattern)
        if pattern.pk:
            logger.info("Creating formset for existing pattern")
            formset = SplitFormSet(instance=pattern)
        else:
            logger.info("Creating empty formset for new pattern")
            # For new patterns, create formset without instance to avoid relationship access
            formset = SplitFormSet()

    logger.info("Rendering template")
    return render(request, 'behavioural/pattern_manage.html', {
        'form': form,
        'formset': formset,
        'is_edit': bool(pk),
    })

# BehavioralPatternSplit Views
class BehavioralPatternSplitListView(ListView):
    model = BehavioralPatternSplit
    template_name = 'behavioural/split_list.html'
    context_object_name = 'splits'

    def get_queryset(self):
        self.pattern = get_object_or_404(BehavioralPattern, pk=self.kwargs['pattern_id'])
        return BehavioralPatternSplit.objects.filter(pattern=self.pattern)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['pattern'] = self.pattern
        return context

class BehavioralPatternSplitCreateView(CreateView):
    model = BehavioralPatternSplit
    form_class = BehavioralPatternSplitForm
    template_name = 'behavioural/split_form.html'

    def form_valid(self, form):
        pattern = get_object_or_404(BehavioralPattern, pk=self.kwargs['pattern_id'])
        form.instance.pattern = pattern
        return super().form_valid(form)

    def get_success_url(self):
        return reverse('alm_app:split-list', kwargs={'pattern_id': self.kwargs['pattern_id']})

class BehavioralPatternSplitUpdateView(UpdateView):
    model = BehavioralPatternSplit
    form_class = BehavioralPatternSplitForm
    template_name = 'behavioural/split_form.html'

    def get_success_url(self):
        return reverse('alm_app:split-list', kwargs={'pattern_id': self.object.pattern.id})

class BehavioralPatternSplitDeleteView(DeleteView):
    model = BehavioralPatternSplit
    template_name = 'behavioural/split_confirm_delete.html'

    def get_success_url(self):
        return reverse('alm_app:split-list', kwargs={'pattern_id': self.object.pattern.id})

# JSON delete endpoint (no HTML)
def pattern_delete_api(request, pk: int):
    if request.method != 'POST':
        return HttpResponseNotAllowed(['POST'])
    try:
        pattern = get_object_or_404(BehavioralPattern, pk=pk)
        v_prod_type = pattern.v_prod_type
        pattern.delete()
        success_msg = f'Pattern "{v_prod_type}" deleted successfully.'
        # Always add to Django messages so the next page can show it
        messages.success(request, success_msg)
        redirect_url = reverse('alm_app:pattern-list')
        # If AJAX (fetch/XHR) keep JSON contract; client should navigate to redirect_url
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({
                'ok': True,
                'message': success_msg,
                'redirect_url': redirect_url
            })
        # Otherwise, perform a server-side redirect so message shows
        return HttpResponseRedirect(redirect_url)
    except Exception as e:
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return JsonResponse({'ok': False, 'error': str(e)}, status=400)
        messages.error(request, str(e))
        return HttpResponseRedirect(reverse('alm_app:pattern-list'))
