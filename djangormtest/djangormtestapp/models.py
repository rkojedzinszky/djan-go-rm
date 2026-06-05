from django.db import models
from django.contrib.postgres.fields import ArrayField
from netfields import InetAddressField, MACAddressField


class Author(models.Model):
    """Test model for ForeignKey relationships and basic types"""
    name = models.CharField(max_length=100)
    email = models.CharField(max_length=100, null=True, blank=True)
    is_active = models.BooleanField(default=True, null=True, blank=True)
    birth_date = models.DateField(null=True, blank=True)
    bio = models.CharField(max_length=500, null=True, blank=True)
    
    class Meta:
        app_label = 'djangormtestapp'


class Article(models.Model):
    """Test model with various field types and nullable fields"""
    title = models.CharField(max_length=200)
    content = models.CharField(max_length=5000, null=True, blank=True)
    author = models.ForeignKey(Author, on_delete=models.CASCADE)
    
    # Numeric types
    view_count = models.IntegerField(default=0, null=True, blank=True)
    priority = models.SmallIntegerField(default=1, null=True, blank=True)
    large_number = models.BigIntegerField(null=True, blank=True)
    rating = models.FloatField(null=True, blank=True)
    
    # DateTime types
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now=True, null=True, blank=True)
    publish_date = models.DateField(null=True, blank=True)
    publish_time = models.TimeField(null=True, blank=True)
    
    # Boolean types
    is_published = models.BooleanField(default=False, null=True, blank=True)
    
    # Network types
    author_ip = models.GenericIPAddressField(null=True, blank=True)
    author_inet = InetAddressField(null=True, blank=True)
    author_mac = MACAddressField(null=True, blank=True)
    
    # Duration type
    read_duration = models.DurationField(null=True, blank=True)
    
    class Meta:
        app_label = 'djangormtestapp'


class Review(models.Model):
    """Test model with array fields and additional nullable types"""
    article = models.ForeignKey(Article, on_delete=models.CASCADE)
    reviewer_name = models.CharField(max_length=100)
    rating = models.IntegerField(null=True, blank=True)
    comment = models.CharField(max_length=1000, null=True, blank=True)
    
    # Array fields
    tags = ArrayField(models.CharField(max_length=50), null=True, blank=True)
    scores = ArrayField(models.IntegerField(), null=True, blank=True)
    
    # Additional nullable fields
    reviewer_ip = models.GenericIPAddressField(null=True, blank=True)
    reviewer_inet = InetAddressField(null=True, blank=True)
    reviewer_mac = MACAddressField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True, null=True, blank=True)
    
    class Meta:
        app_label = 'djangormtestapp'
