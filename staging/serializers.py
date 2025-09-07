from rest_framework import serializers
from .models import FirstDayProduct

class FirstDayProductSerializer(serializers.ModelSerializer):
    class Meta:
        model = FirstDayProduct
        fields = '__all__'
        read_only_fields = ('created_at', 'updated_at') 