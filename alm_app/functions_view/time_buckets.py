from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from ..models import TimeBuckets

@login_required
def time_bucket_list(request):
    """View to list all time buckets"""
    buckets = TimeBuckets.objects.all().order_by('serial_number')
    return render(request, 'time_buckets/list.html', {
        'buckets': buckets
    })

@login_required
def time_bucket_manage(request):
    """View to create or edit time buckets based on whether they exist"""
    buckets = TimeBuckets.objects.all().order_by('serial_number')
    is_create = not buckets.exists()
    
    if request.method == 'POST':
        try:
            bucket_count = int(request.POST.get('bucket_count', 0))
            
            # Update or create buckets
            existing_buckets = list(buckets)
            
            for i in range(bucket_count):
                bucket_data = {
                    'serial_number': i + 1,
                    'start_date': request.POST.get(f'start_date_{i}'),
                    'end_date': request.POST.get(f'end_date_{i}'),
                    'frequency': request.POST.get(f'frequency_{i}'),
                    'multiplier': request.POST.get(f'multiplier_{i}')
                }
                
                if i < len(existing_buckets):
                    # Update existing bucket
                    for key, value in bucket_data.items():
                        setattr(existing_buckets[i], key, value)
                    existing_buckets[i].save()
                else:
                    # Create new bucket
                    TimeBuckets.objects.create(**bucket_data)
            
            # Delete extra buckets if bucket_count is less than existing
            if bucket_count < len(existing_buckets):
                for bucket in existing_buckets[bucket_count:]:
                    bucket.delete()
            
            messages.success(request, 'Time buckets saved successfully.')
            return redirect('alm_app:time_bucket_list')
        except Exception as e:
            messages.error(request, f'Error saving time buckets: {str(e)}')
    
    template = 'time_buckets/create.html' if is_create else 'time_buckets/create.html'
    return render(request, template, {
        'buckets': buckets,
        'is_create': is_create
    })

@login_required
def time_bucket_delete(request):
    """View to delete all time buckets"""
    if request.method == 'POST':
        try:
            TimeBuckets.objects.all().delete()
            messages.success(request, 'Time buckets deleted successfully.')
        except Exception as e:
            messages.error(request, f'Error deleting time buckets: {str(e)}')
    
    return redirect('alm_app:time_bucket_list')
