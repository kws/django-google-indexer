#!/usr/bin/env python
"""
Example Django views and API endpoints for email index maintenance.

This file demonstrates how to integrate the maintenance service methods
into Django views, API endpoints, or admin actions.
"""

from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib.admin.actions import action
from django.contrib import messages
from django.shortcuts import redirect
from django.urls import reverse
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAdminUser
from rest_framework.response import Response
from rest_framework import status

from google_email_indexer.email_indexing_service import EmailIndexingService


# Django Views Example
@require_http_methods(["GET"])
def index_health_check(request):
    """
    Simple health check endpoint for monitoring.
    
    Usage: GET /api/email-index/health/
    """
    try:
        validation_results = EmailIndexingService.validate_index()
        
        health_data = {
            'status': 'healthy' if validation_results['is_valid'] else 'issues_found',
            'total_messages': validation_results['total_messages'],
            'total_issues': validation_results['total_issues'],
            'missing_messages': validation_results['missing_messages'],
            'orphaned_emails': validation_results['orphaned_emails'],
            'inconsistent_counts': validation_results['inconsistent_counts']
        }
        
        return JsonResponse(health_data)
        
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        }, status=500)


@require_http_methods(["POST"])
def run_maintenance(request):
    """
    Run maintenance operations via API.
    
    Usage: POST /api/email-index/maintenance/
    Body: {
        "operations": ["validate", "fix_missing", "cleanup"],
        "account_email": "user@example.com"  // optional
    }
    """
    try:
        data = request.POST if request.method == 'POST' else request.GET
        operations = data.getlist('operations') or ['validate']
        account_email = data.get('account_email')
        
        results = {}
        
        # Run requested operations
        if 'validate' in operations:
            results['validation'] = EmailIndexingService.validate_index(account_email)
        
        if 'fix_missing' in operations:
            results['fix_missing'] = EmailIndexingService.fix_missing_entries(
                account_email=account_email,
                batch_size=1000
            )
        
        if 'cleanup' in operations:
            results['cleanup'] = EmailIndexingService.maintenance_cleanup()
        
        if 'statistics' in operations:
            results['statistics'] = EmailIndexingService.get_index_statistics(account_email)
        
        return JsonResponse({
            'success': True,
            'results': results
        })
        
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)


# Django REST Framework API Views
@api_view(['GET'])
@permission_classes([IsAdminUser])
def api_index_statistics(request):
    """
    Get detailed index statistics.
    
    Usage: GET /api/email-index/statistics/
    Query params: account_email (optional)
    """
    try:
        account_email = request.query_params.get('account_email')
        stats = EmailIndexingService.get_index_statistics(account_email)
        
        return Response({
            'success': True,
            'data': stats
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@api_view(['POST'])
@permission_classes([IsAdminUser])
def api_run_maintenance(request):
    """
    Run maintenance operations via REST API.
    
    Usage: POST /api/email-index/maintenance/
    Body: {
        "operations": ["validate", "fix_missing", "cleanup"],
        "account_email": "user@example.com",
        "batch_size": 1000
    }
    """
    try:
        operations = request.data.get('operations', ['validate'])
        account_email = request.data.get('account_email')
        batch_size = request.data.get('batch_size', 1000)
        
        results = {}
        
        # Run requested operations
        if 'validate' in operations:
            results['validation'] = EmailIndexingService.validate_index(account_email)
        
        if 'fix_missing' in operations:
            results['fix_missing'] = EmailIndexingService.fix_missing_entries(
                account_email=account_email,
                batch_size=batch_size
            )
        
        if 'cleanup' in operations:
            results['cleanup'] = EmailIndexingService.maintenance_cleanup()
        
        if 'statistics' in operations:
            results['statistics'] = EmailIndexingService.get_index_statistics(account_email)
        
        return Response({
            'success': True,
            'results': results
        })
        
    except Exception as e:
        return Response({
            'success': False,
            'error': str(e)
        }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# Django Admin Actions Example
def admin_maintenance_actions(modeladmin, request, queryset):
    """
    Admin action for running maintenance on selected accounts.
    
    Add this to your admin.py:
    
    @admin.register(YourAccountModel)
    class YourAccountModelAdmin(admin.ModelAdmin):
        actions = ['run_maintenance']
        
        def run_maintenance(self, request, queryset):
            for account in queryset:
                try:
                    # Run maintenance for this account
                    validation_results = EmailIndexingService.validate_index(account.email)
                    
                    if validation_results['missing_messages'] > 0:
                        EmailIndexingService.fix_missing_entries(account_email=account.email)
                    
                    if validation_results['orphaned_emails'] > 0:
                        EmailIndexingService.maintenance_cleanup()
                    
                    messages.success(request, f'Maintenance completed for {account.email}')
                    
                except Exception as e:
                    messages.error(request, f'Maintenance failed for {account.email}: {e}')
            
            return redirect(reverse('admin:your_app_yourmodel_changelist'))
    
    run_maintenance.short_description = "Run email index maintenance"
    """
    pass


# Celery Task Integration Example
def create_celery_maintenance_tasks():
    """
    Example Celery tasks for background maintenance.
    
    Add this to your tasks.py:
    """
    
    from celery import shared_task
    from django.core.mail import send_mail
    from django.conf import settings
    
    @shared_task
    def daily_maintenance_task():
        """Daily maintenance task"""
        try:
            # Run validation
            validation_results = EmailIndexingService.validate_index()
            
            # Fix issues if found
            if validation_results['missing_messages'] > 0:
                EmailIndexingService.fix_missing_entries()
            
            if validation_results['orphaned_emails'] > 0:
                EmailIndexingService.maintenance_cleanup()
            
            # Send notification if issues were found
            if validation_results['total_issues'] > 0:
                send_mail(
                    subject=f'Email Index Maintenance - {validation_results["total_issues"]} issues fixed',
                    message=f"""
                    Daily maintenance completed:
                    - Missing messages: {validation_results['missing_messages']}
                    - Orphaned emails: {validation_results['orphaned_emails']}
                    - Inconsistent counts: {validation_results['inconsistent_counts']}
                    """,
                    from_email=settings.DEFAULT_FROM_EMAIL,
                    recipient_list=[settings.ADMIN_EMAIL],
                )
            
            return {
                'success': True,
                'issues_fixed': validation_results['total_issues']
            }
            
        except Exception as e:
            # Send error notification
            send_mail(
                subject='Email Index Maintenance Failed',
                message=f'Daily maintenance failed: {e}',
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[settings.ADMIN_EMAIL],
            )
            
            return {
                'success': False,
                'error': str(e)
            }
    
    @shared_task
    def weekly_maintenance_task():
        """Weekly comprehensive maintenance task"""
        try:
            # Full cleanup
            cleanup_results = EmailIndexingService.maintenance_cleanup()
            
            # Get comprehensive statistics
            stats = EmailIndexingService.get_index_statistics()
            
            # Send weekly report
            send_mail(
                subject='Weekly Email Index Report',
                message=f"""
                Weekly maintenance completed:
                - Orphaned emails removed: {cleanup_results['orphaned_emails_removed']}
                - Total messages: {stats['total_messages']}
                - Total indexed emails: {stats['total_indexed_emails']}
                - Total relationships: {stats['total_relationships']}
                """,
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[settings.ADMIN_EMAIL],
            )
            
            return {
                'success': True,
                'cleanup_results': cleanup_results,
                'statistics': stats
            }
            
        except Exception as e:
            send_mail(
                subject='Weekly Email Index Maintenance Failed',
                message=f'Weekly maintenance failed: {e}',
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[settings.ADMIN_EMAIL],
            )
            
            return {
                'success': False,
                'error': str(e)
            }


# URL Configuration Example
def get_url_patterns():
    """
    Example URL patterns for the maintenance endpoints.
    
    Add to your urls.py:
    """
    
    from django.urls import path
    
    urlpatterns = [
        # Health check endpoint
        path('api/email-index/health/', index_health_check, name='email_index_health'),
        
        # Maintenance endpoint
        path('api/email-index/maintenance/', run_maintenance, name='email_index_maintenance'),
        
        # REST API endpoints (if using DRF)
        path('api/v1/email-index/statistics/', api_index_statistics, name='api_email_index_statistics'),
        path('api/v1/email-index/maintenance/', api_run_maintenance, name='api_email_index_maintenance'),
    ]
    
    return urlpatterns


# Monitoring Integration Example
def setup_monitoring_integration():
    """
    Example integration with monitoring systems.
    """
    
    # For Prometheus metrics
    from prometheus_client import Gauge, Counter
    
    # Define metrics
    email_index_issues = Gauge('email_index_issues_total', 'Total issues in email index')
    email_index_messages = Gauge('email_index_messages_total', 'Total messages in email index')
    email_index_emails = Gauge('email_index_emails_total', 'Total indexed emails')
    maintenance_runs = Counter('email_index_maintenance_runs_total', 'Total maintenance runs')
    maintenance_errors = Counter('email_index_maintenance_errors_total', 'Total maintenance errors')
    
    def update_monitoring_metrics():
        """Update monitoring metrics"""
        try:
            validation_results = EmailIndexingService.validate_index()
            stats = EmailIndexingService.get_index_statistics()
            
            # Update gauges
            email_index_issues.set(validation_results['total_issues'])
            email_index_messages.set(stats['total_messages'])
            email_index_emails.set(stats['total_indexed_emails'])
            
        except Exception as e:
            maintenance_errors.inc()
            raise
    
    def record_maintenance_run():
        """Record a maintenance run"""
        maintenance_runs.inc()
    
    return {
        'update_metrics': update_monitoring_metrics,
        'record_run': record_maintenance_run
    }


if __name__ == '__main__':
    # Example usage in a script
    print("Email Index Maintenance API Examples")
    print("=" * 50)
    
    # Health check
    print("1. Health Check:")
    try:
        validation_results = EmailIndexingService.validate_index()
        print(f"   Status: {'Healthy' if validation_results['is_valid'] else 'Issues Found'}")
        print(f"   Issues: {validation_results['total_issues']}")
    except Exception as e:
        print(f"   Error: {e}")
    
    # Statistics
    print("\n2. Statistics:")
    try:
        stats = EmailIndexingService.get_index_statistics()
        print(f"   Messages: {stats['total_messages']}")
        print(f"   Emails: {stats['total_indexed_emails']}")
        print(f"   Relationships: {stats['total_relationships']}")
    except Exception as e:
        print(f"   Error: {e}")
    
    print("\nSee the documentation for more examples and integration patterns.") 