# properties/utils.py
from django.core.cache import cache
from django.db.models import QuerySet
from typing import Optional, List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def get_all_properties() -> QuerySet:
    """
    Retrieve all properties with low-level Redis caching.
    
    Returns:
        QuerySet: All Property objects, either from cache or database
    """
    cache_key = 'all_properties'
    
    # Try to get from cache
    logger.info(f"Attempting to retrieve properties from cache with key: {cache_key}")
    cached_data = cache.get(cache_key)
    
    if cached_data is not None:
        logger.info(f"✅ Cache HIT for key: {cache_key}")
        
        # Log cache statistics
        try:
            # Get TTL for cache key
            ttl = cache.ttl(cache_key)
            if ttl:
                logger.info(f"   Cache TTL remaining: {ttl} seconds")
        except:
            pass
        
        return cached_data
    
    # Cache miss - fetch from database
    logger.info(f"❌ Cache MISS for key: {cache_key}. Fetching from database...")
    
    try:
        # Fetch all properties from database with optimization
        from .models import Property
        
        start_time = datetime.now()
        queryset = Property.objects.all().select_related().order_by('-created_at')
        
        # Force evaluation of queryset to cache the results
        properties_list = list(queryset)
        
        end_time = datetime.now()
        fetch_time = (end_time - start_time).total_seconds()
        
        logger.info(f"   Database fetch completed in {fetch_time:.3f} seconds")
        logger.info(f"   Retrieved {len(properties_list)} properties")
        
        # Store in cache for 1 hour (3600 seconds)
        logger.info(f"   Storing in cache with TTL: 3600 seconds")
        cache.set(cache_key, queryset, timeout=3600)
        
        # Also store metadata
        cache_meta_key = f"{cache_key}_meta"
        metadata = {
            'cached_at': datetime.now().isoformat(),
            'count': len(properties_list),
            'fetch_time': fetch_time,
            'source': 'database'
        }
        cache.set(cache_meta_key, metadata, timeout=3600)
        
        return queryset
        
    except Exception as e:
        logger.error(f"Error fetching properties: {e}")
        raise


def get_properties_by_location(location: str) -> QuerySet:
    """
    Get properties by location with caching.
    
    Args:
        location (str): Location to filter properties
        
    Returns:
        QuerySet: Filtered properties
    """
    cache_key = f'properties_location_{location.lower().replace(" ", "_")}'
    
    cached_data = cache.get(cache_key)
    
    if cached_data is not None:
        logger.info(f"Cache HIT for location: {location}")
        return cached_data
    
    logger.info(f"Cache MISS for location: {location}")
    
    from .models import Property
    queryset = Property.objects.filter(
        location__icontains=location
    ).order_by('-created_at')
    
    # Cache for 30 minutes (1800 seconds)
    cache.set(cache_key, queryset, timeout=1800)
    
    return queryset


def get_properties_by_price_range(min_price: float, max_price: float) -> QuerySet:
    """
    Get properties within a price range with caching.
    
    Args:
        min_price (float): Minimum price
        max_price (float): Maximum price
        
    Returns:
        QuerySet: Filtered properties
    """
    cache_key = f'properties_price_{min_price}_{max_price}'
    
    cached_data = cache.get(cache_key)
    
    if cached_data is not None:
        logger.info(f"Cache HIT for price range: ${min_price}-${max_price}")
        return cached_data
    
    logger.info(f"Cache MISS for price range: ${min_price}-${max_price}")
    
    from .models import Property
    queryset = Property.objects.filter(
        price__gte=min_price,
        price__lte=max_price
    ).order_by('price')
    
    # Cache for 15 minutes (900 seconds)
    cache.set(cache_key, queryset, timeout=900)
    
    return queryset


def invalidate_property_cache():
    """
    Invalidate all property-related cache keys.
    """
    from django.core.cache import cache
    
    # List of cache key patterns to invalidate
    cache_patterns = [
        'all_properties',
        'all_properties_meta',
        'properties_location_*',
        'properties_price_*',
        'property_*',  # Individual property cache
    ]
    
    invalidated_count = 0
    
    for pattern in cache_patterns:
        try:
            # Get all keys matching pattern
            keys = cache.keys(pattern)
            if keys:
                cache.delete_many(keys)
                invalidated_count += len(keys)
                logger.info(f"Invalidated {len(keys)} keys matching pattern: {pattern}")
        except Exception as e:
            logger.warning(f"Could not invalidate pattern {pattern}: {e}")
    
    logger.info(f"Total cache keys invalidated: {invalidated_count}")
    return invalidated_count


def get_cache_stats() -> dict:
    """
    Get statistics about property cache.
    
    Returns:
        dict: Cache statistics
    """
    stats = {
        'all_properties': {
            'cached': False,
            'ttl': None,
            'metadata': None,
        },
        'locations': {},
        'price_ranges': {},
    }
    
    # Check all_properties cache
    cache_key = 'all_properties'
    if cache.get(cache_key) is not None:
        stats['all_properties']['cached'] = True
        ttl = cache.ttl(cache_key)
        if ttl:
            stats['all_properties']['ttl'] = ttl
        
        # Get metadata
        meta_key = f"{cache_key}_meta"
        metadata = cache.get(meta_key)
        if metadata:
            stats['all_properties']['metadata'] = metadata
    
    return stats


class PropertyCacheManager:
    """
    Advanced cache manager for properties with additional features.
    """
    
    @staticmethod
    def get_all_with_fallback():
        """
        Get all properties with cache fallback strategy.
        """
        try:
            return get_all_properties()
        except Exception as e:
            logger.error(f"Cache failed, fetching from database directly: {e}")
            from .models import Property
            return Property.objects.all()
    
    @staticmethod
    def warm_cache():
        """
        Warm up the cache by pre-loading frequently accessed data.
        """
        logger.info("Warming up property cache...")
        
        # Pre-load all properties
        get_all_properties()
        
        # Pre-load by common locations
        common_locations = ['New York', 'Los Angeles', 'Chicago', 'Miami', 'Seattle']
        for location in common_locations:
            get_properties_by_location(location)
        
        # Pre-load common price ranges
        price_ranges = [
            (0, 500000),
            (500000, 1000000),
            (1000000, 5000000)
        ]
        for min_price, max_price in price_ranges:
            get_properties_by_price_range(min_price, max_price)
        
        logger.info("Cache warm-up completed")
    
    @staticmethod
    def clear_pattern(pattern: str):
        """
        Clear cache keys matching a pattern.
        
        Args:
            pattern (str): Pattern to match cache keys
        """
        try:
            keys = cache.keys(pattern)
            if keys:
                cache.delete_many(keys)
                logger.info(f"Cleared {len(keys)} keys matching pattern: {pattern}")
                return len(keys)
        except Exception as e:
            logger.error(f"Error clearing pattern {pattern}: {e}")
            return 0
