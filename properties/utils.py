# properties/utils.py
from django.core.cache import cache
from django.db.models import QuerySet
from typing import Optional, List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

def get_all_properties():
    pass

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

# properties/utils.py
from django.core.cache import cache
from django.db.models import QuerySet
from typing import Dict, Any, Optional, Tuple
import logging
import redis
from datetime import datetime, timedelta
import json

logger = logging.getLogger(__name__)

def get_redis_cache_metrics() -> Dict[str, Any]:
    """
    Retrieve and analyze Redis cache hit/miss metrics.
    
    Returns:
        Dictionary containing cache metrics including hit ratio
    """
    metrics = {
        'status': 'error',
        'message': '',
        'hits': 0,
        'misses': 0,
        'hit_ratio': 0.0,
        'hit_percentage': 0.0,
        'total_operations': 0,
        'keys_count': 0,
        'memory_usage': 'N/A',
        'uptime': 'N/A',
        'connected_clients': 0,
        'cache_backend': 'Unknown',
        'timestamp': datetime.now().isoformat(),
    }
    
    try:
        # Get Redis connection from Django cache
        redis_client = _get_redis_client()
        
        if not redis_client:
            metrics['message'] = 'Could not connect to Redis'
            logger.error(metrics['message'])
            return metrics
        
        # Get Redis INFO command output
        redis_info = redis_client.info()
        
        # Extract cache metrics
        hits = redis_info.get('keyspace_hits', 0)
        misses = redis_info.get('keyspace_misses', 0)
        total_ops = hits + misses
        
        # Calculate hit ratio
        hit_ratio = hits / total_ops if total_ops > 0 else 0
        
        # Populate metrics
        metrics.update({
            'status': 'success',
            'hits': hits,
            'misses': misses,
            'total_operations': total_ops,
            'hit_ratio': round(hit_ratio, 4),
            'hit_percentage': round(hit_ratio * 100, 2),
            'keys_count': redis_info.get('db1', {}).get('keys', 0),
            'memory_usage': _format_bytes(redis_info.get('used_memory', 0)),
            'memory_peak': _format_bytes(redis_info.get('used_memory_peak', 0)),
            'uptime': _format_seconds(redis_info.get('uptime_in_seconds', 0)),
            'connected_clients': redis_info.get('connected_clients', 0),
            'cache_backend': str(type(cache._cache)),
            'redis_version': redis_info.get('redis_version', 'N/A'),
            'evicted_keys': redis_info.get('evicted_keys', 0),
            'expired_keys': redis_info.get('expired_keys', 0),
            'total_commands_processed': redis_info.get('total_commands_processed', 0),
            'instantaneous_ops_per_sec': redis_info.get('instantaneous_ops_per_sec', 0),
            'keyspace_hits': hits,
            'keyspace_misses': misses,
        })
        
        # Log the metrics
        logger.info(
            f"Redis Cache Metrics - "
            f"Hits: {hits}, Misses: {misses}, "
            f"Hit Ratio: {metrics['hit_percentage']}%, "
            f"Keys: {metrics['keys_count']}, "
            f"Memory: {metrics['memory_usage']}"
        )
        
        # Store metrics history (last 24 hours)
        _store_metrics_history(metrics)
        
        # Performance evaluation
        metrics['performance'] = _evaluate_performance(metrics)
        
    except redis.ConnectionError as e:
        metrics['message'] = f'Redis connection error: {str(e)}'
        logger.error(metrics['message'])
    except Exception as e:
        metrics['message'] = f'Error getting cache metrics: {str(e)}'
        logger.error(metrics['message'])
    
    return metrics


def _get_redis_client() -> Optional[redis.Redis]:
    """
    Get Redis client from Django cache backend.
    
    Returns:
        Redis client or None if not available
    """
    try:
        # Try to get Redis client from django-redis
        if hasattr(cache, '_client'):
            return cache._client
        
        # Try alternative ways to get Redis client
        if hasattr(cache, '_cache'):
            cache_backend = cache._cache
            if hasattr(cache_backend, 'client'):
                return cache_backend.client
            elif hasattr(cache_backend, '_client'):
                return cache_backend._client
        
        # Try to create Redis client from settings
        from django.conf import settings
        cache_settings = settings.CACHES['default']
        
        if 'LOCATION' in cache_settings:
            location = cache_settings['LOCATION']
            if location.startswith('redis://'):
                import redis
                return redis.from_url(location)
        
        logger.warning("Could not get Redis client from cache backend")
        return None
        
    except Exception as e:
        logger.error(f"Error getting Redis client: {e}")
        return None


def _format_bytes(bytes_size: int) -> str:
    """
    Format bytes to human readable string.
    
    Args:
        bytes_size: Size in bytes
        
    Returns:
        Formatted string (e.g., "1.5 MB")
    """
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} TB"


def _format_seconds(seconds: int) -> str:
    """
    Format seconds to human readable time.
    
    Args:
        seconds: Time in seconds
        
    Returns:
        Formatted string (e.g., "1d 2h 3m")
    """
    days = seconds // 86400
    hours = (seconds % 86400) // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")
    
    return " ".join(parts)


def _store_metrics_history(metrics: Dict[str, Any]) -> None:
    """
    Store metrics history for trend analysis.
    
    Args:
        metrics: Current metrics
    """
    try:
        history_key = 'cache_metrics_history'
        max_history = 1440  # 24 hours of minute-level data
        
        # Get existing history
        history = cache.get(history_key) or []
        
        # Add current metrics (store only essential data)
        history_entry = {
            'timestamp': metrics['timestamp'],
            'hit_percentage': metrics['hit_percentage'],
            'hits': metrics['hits'],
            'misses': metrics['misses'],
            'keys_count': metrics['keys_count'],
            'connected_clients': metrics['connected_clients'],
        }
        
        history.append(history_entry)
        
        # Keep only last max_history entries
        if len(history) > max_history:
            history = history[-max_history:]
        
        # Store back to cache
        cache.set(history_key, history, timeout=None)  # Persistent
        
    except Exception as e:
        logger.warning(f"Could not store metrics history: {e}")


def _evaluate_performance(metrics: Dict[str, Any]) -> Dict[str, Any]:
    """
    Evaluate cache performance based on metrics.
    
    Args:
        metrics: Cache metrics
        
    Returns:
        Performance evaluation
    """
    hit_percentage = metrics['hit_percentage']
    hit_ratio = metrics['hit_ratio']
    
    evaluation = {
        'grade': 'Unknown',
        'message': '',
        'recommendations': [],
    }
    
    if hit_percentage >= 90:
        evaluation['grade'] = 'Excellent'
        evaluation['message'] = 'Cache is performing optimally'
    elif hit_percentage >= 80:
        evaluation['grade'] = 'Good'
        evaluation['message'] = 'Cache performance is good'
        if hit_percentage < 85:
            evaluation['recommendations'].append(
                'Consider increasing cache timeout for frequently accessed data'
            )
    elif hit_percentage >= 70:
        evaluation['grade'] = 'Fair'
        evaluation['message'] = 'Cache performance is acceptable but could be improved'
        evaluation['recommendations'].extend([
            'Review cache keys and expiration times',
            'Consider implementing cache warming for hot data',
            'Monitor cache patterns for optimization opportunities',
        ])
    else:
        evaluation['grade'] = 'Poor'
        evaluation['message'] = 'Cache performance needs improvement'
        evaluation['recommendations'].extend([
            'Investigate why cache misses are high',
            'Review cache invalidation strategies',
            'Consider increasing cache size or implementing multi-level caching',
            'Profile application to identify hot data patterns',
        ])
    
    # Check memory usage
    try:
        memory_str = metrics['memory_usage']
        if 'MB' in memory_str:
            mb = float(memory_str.split()[0])
            if mb > 100:  # More than 100MB
                evaluation['recommendations'].append(
                    f'Cache using {memory_str}. Consider memory optimization.'
                )
    except:
        pass
    
    return evaluation


def get_cache_metrics_trend() -> Dict[str, Any]:
    """
    Get cache metrics trend over time.
    
    Returns:
        Trend analysis
    """
    try:
        history_key = 'cache_metrics_history'
        history = cache.get(history_key) or []
        
        if not history:
            return {
                'status': 'no_data',
                'message': 'No historical data available',
                'trend': 'unknown',
            }
        
        # Calculate trend based on last 10 entries
        recent = history[-10:] if len(history) >= 10 else history
        
        hit_percentages = [entry['hit_percentage'] for entry in recent]
        
        if len(hit_percentages) < 2:
            return {
                'status': 'insufficient_data',
                'message': 'Not enough data for trend analysis',
                'average_hit_rate': sum(hit_percentages) / len(hit_percentages),
            }
        
        # Calculate simple linear trend
        first = hit_percentages[0]
        last = hit_percentages[-1]
        trend_value = last - first
        
        if trend_value > 5:
            trend = 'improving_significantly'
            trend_message = f'Hit rate improved by {trend_value:.1f}%'
        elif trend_value > 1:
            trend = 'improving'
            trend_message = f'Hit rate improved by {trend_value:.1f}%'
        elif trend_value < -5:
            trend = 'declining_significantly'
            trend_message = f'Hit rate declined by {abs(trend_value):.1f}%'
        elif trend_value < -1:
            trend = 'declining'
            trend_message = f'Hit rate declined by {abs(trend_value):.1f}%'
        else:
            trend = 'stable'
            trend_message = 'Hit rate is stable'
        
        return {
            'status': 'success',
            'trend': trend,
            'trend_message': trend_message,
            'trend_value': trend_value,
            'current_hit_rate': last,
            'average_hit_rate': sum(hit_percentages) / len(hit_percentages),
            'data_points': len(history),
            'time_period': f'{len(history)} minutes',
        }
        
    except Exception as e:
        logger.error(f"Error getting metrics trend: {e}")
        return {
            'status': 'error',
            'message': str(e),
        }


def reset_cache_metrics() -> Dict[str, Any]:
    """
    Reset Redis cache statistics (INFO command stats).
    
    Returns:
        Result of reset operation
    """
    try:
        redis_client = _get_redis_client()
        
        if not redis_client:
            return {
                'status': 'error',
                'message': 'Could not connect to Redis',
            }
        
        # Execute RESETSTAT command
        redis_client.execute_command('CONFIG', 'RESETSTAT')
        
        # Clear local metrics history
        cache.delete('cache_metrics_history')
        
        logger.info("Cache metrics reset successfully")
        
        return {
            'status': 'success',
            'message': 'Cache metrics reset successfully',
            'timestamp': datetime.now().isoformat(),
        }
        
    except Exception as e:
        logger.error(f"Error resetting cache metrics: {e}")
        return {
            'status': 'error',
            'message': str(e),
        }


def get_detailed_cache_analysis() -> Dict[str, Any]:
    """
    Get detailed cache analysis with recommendations.
    
    Returns:
        Comprehensive cache analysis
    """
    # Get current metrics
    metrics = get_redis_cache_metrics()
    
    if metrics['status'] != 'success':
        return metrics
    
    # Get trend analysis
    trend = get_cache_metrics_trend()
    
    # Get property-specific cache info
    property_cache_info = _get_property_cache_info()
    
    # Compile comprehensive analysis
    analysis = {
        'summary': {
            'hit_rate': metrics['hit_percentage'],
            'performance_grade': metrics.get('performance', {}).get('grade', 'Unknown'),
            'total_keys': metrics['keys_count'],
            'memory_usage': metrics['memory_usage'],
            'trend': trend.get('trend_message', 'Unknown'),
        },
        'metrics': metrics,
        'trend_analysis': trend,
        'property_cache_info': property_cache_info,
        'timestamp': datetime.now().isoformat(),
        'recommendations': metrics.get('performance', {}).get('recommendations', []),
    }
    
    # Add custom recommendations based on property cache info
    if property_cache_info['cached_count'] == 0:
        analysis['recommendations'].append(
            'No properties are currently cached. Consider implementing cache warming.'
        )
    
    if property_cache_info['cached_count'] > 0 and metrics['hit_percentage'] < 70:
        analysis['recommendations'].append(
            f'Only {property_cache_info["cached_count"]} properties are cached. '
            f'Consider caching more properties or implementing smarter caching strategies.'
        )
    
    return analysis


def _get_property_cache_info() -> Dict[str, Any]:
    """
    Get information about property-related cache entries.
    
    Returns:
        Property cache information
    """
    try:
        redis_client = _get_redis_client()
        
        if not redis_client:
            return {'error': 'Could not connect to Redis'}
        
        # Count property-related cache keys
        property_patterns = ['all_properties*', 'property_*', 'properties_*']
        
        cached_keys = []
        total_cached = 0
        
        for pattern in property_patterns:
            try:
                keys = redis_client.keys(pattern)
                cached_keys.extend(keys)
                total_cached += len(keys)
            except:
                pass
        
        # Check if main properties cache exists
        main_cache_exists = redis_client.exists('all_properties')
        
        return {
            'cached_count': total_cached,
            'main_cache_exists': bool(main_cache_exists),
            'cache_keys': cached_keys[:10],  # First 10 keys
            'total_keys_found': len(cached_keys),
        }
        
    except Exception as e:
        return {'error': str(e)}
