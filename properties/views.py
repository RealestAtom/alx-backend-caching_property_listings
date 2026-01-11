# properties/views.py
from django.shortcuts import render
from django.views.generic import ListView, DetailView
from django.core.cache import cache
from django.utils.decorators import method_decorator
from django.views.decorators.cache import cache_page
from .models import Property
from django.http import JsonResponse


@cache_page(60 * 15)
def property_list(request):
    return JsonResponse({request.data})


class PropertyListView(ListView):
    model = Property
    template_name = 'properties/property_list.html'
    context_object_name = 'properties'
    paginate_by = 10
    
    @cache_page(60 * 5)  # Cache for 5 minutes
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def get_queryset(self):
        cache_key = 'properties_list'
        cached_data = cache.get(cache_key)
        
        if cached_data is not None:
            return cached_data
        
        queryset = Property.objects.filter(status='available').order_by('-created_at')
        cache.set(cache_key, queryset, 60 * 5)  # Cache for 5 minutes
        return queryset

class PropertyDetailView(DetailView):
    model = Property
    template_name = 'properties/property_detail.html'
    context_object_name = 'property'
    
    @method_decorator(cache_page(60 * 15))  # Cache for 15 minutes
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def get_object(self, queryset=None):
        property_id = self.kwargs.get('pk')
        cache_key = f'property_{property_id}'
        cached_property = cache.get(cache_key)
        
        if cached_property is not None:
            return cached_property
        
        property_obj = super().get_object(queryset)
        cache.set(cache_key, property_obj, 60 * 15)  # Cache for 15 minutes
        return property_obj
    # properties/views.py



from django.utils.decorators import method_decorator
from django.views.generic import ListView
from django.core.cache import cache
from .models import Property
import logging

# Configure logger
logger = logging.getLogger(__name__)

# Function-based view with cache_page decorator
@cache_page(60 * 15)  # Cache for 15 minutes
def property_list_view(request):
    """
    Function-based property list view with caching
    """
    properties = Property.objects.all().order_by('-created_at')
    
    # Log cache hit/miss (for debugging)
    logger.info(f"Property list view accessed - Cache key: {request.path}")
    
    return render(request, 'properties/property_list.html', {
        'properties': properties,
        'view_type': 'Function-based view',
        'cache_time': '15 minutes'
    })


# Class-based view with cache_page decorator
class PropertyListView(ListView):
    """
    Class-based property list view with caching
    """
    model = Property
    template_name = 'properties/property_list.html'
    context_object_name = 'properties'
    paginate_by = 10
    
    @method_decorator(cache_page(60 * 15))  # Cache for 15 minutes
    def dispatch(self, *args, **kwargs):
        return super().dispatch(*args, **kwargs)
    
    def get_queryset(self):
        # Order by newest first
        return Property.objects.all().order_by('-created_at')
    
    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        # Add additional context
        context['view_type'] = 'Class-based view'
        context['cache_time'] = '15 minutes'
        context['total_properties'] = Property.objects.count()
        
        # Log for debugging
        logger.info(f"Class-based property list view - Total: {context['total_properties']}")
        
        return context


# Advanced view with manual cache control
def property_list_advanced(request):
    """
    Advanced property list view with manual Redis cache control
    """
    cache_key = 'property_list_advanced'
    
    # Try to get from cache
    cached_data = cache.get(cache_key)
    
    if cached_data is not None:
        logger.info(f"Cache HIT for key: {cache_key}")
        properties, context_data = cached_data
        
        # Add cache indicator to context
        context_data['cache_hit'] = True
        context_data['cached_at'] = cache.ttl(cache_key)  # Time remaining in seconds
        return render(request, 'properties/property_list.html', context_data)
    
    logger.info(f"Cache MISS for key: {cache_key}")
    
    # Cache miss - fetch from database
    properties = Property.objects.all().order_by('-created_at')
    
    # Prepare context
    context_data = {
        'properties': properties,
        'view_type': 'Advanced view with manual caching',
        'cache_time': '15 minutes',
        'cache_hit': False,
        'total_properties': Property.objects.count(),
    }
    
    # Store in cache for 15 minutes (900 seconds)
    cache.set(cache_key, (properties, context_data), timeout=60 * 15)
    
    return render(request, 'properties/property_list.html', context_data)
