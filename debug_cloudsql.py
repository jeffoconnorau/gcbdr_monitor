
import os
import sys
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_cloudsql(project_id, resource_name):
    # Lazy import to handle missing dependency gracefully
    try:
        from googleapiclient import discovery
    except ImportError:
        logger.error("'googleapiclient' module not found. Please install it via: pip install google-api-python-client")
        sys.exit(1)

    logger.info(f"Debugging Cloud SQL for Project: {project_id}, Resource: {resource_name}")
    
    target_project = project_id
    instance_name = resource_name
    
    # Same logic as analyzer.py
    if '/' not in resource_name:
         instance_name = resource_name
    else:
         parts = resource_name.split('/')
         if 'instances' in parts:
             try:
                 idx = parts.index('instances')
                 if idx + 1 < len(parts):
                     instance_name = parts[idx+1]
                 if 'projects' in parts:
                     p_idx = parts.index('projects')
                     if p_idx + 1 < len(parts):
                         target_project = parts[p_idx+1]
             except ValueError:
                 pass
    
    logger.info(f"Parsed -> Project: {target_project}, Instance: {instance_name}")
    
    try:
        service = discovery.build('sqladmin', 'v1', cache_discovery=False)
        request = service.instances().get(project=target_project, instance=instance_name)
        response = request.execute()
        
        logger.info("API Call Successful.")
        logger.info(f"Response keys: {list(response.keys())}")
        
        if 'settings' in response:
            settings = response['settings']
            logger.info(f"Settings keys: {list(settings.keys())}")
            if 'dataDiskSizeGb' in settings:
                logger.info(f"FOUND dataDiskSizeGb: {settings['dataDiskSizeGb']}")
            else:
                logger.warning("dataDiskSizeGb NOT FOUND in settings.")
        else:
            logger.warning("Settings NOT FOUND in response.")
            
        # Check for other potential size fields
        # 'diskEncryptionStatus', 'diskAutoresize' ...
        
    except Exception as e:
        logger.error(f"API Call Failed: {e}")
        logger.error("Please ensure the service account has 'Cloud SQL Viewer' (roles/cloudsql.viewer) permission.")

if __name__ == "__main__":
    try:
        import argparse
    except ImportError:
        print("Error: argparse module missing (this should be standard in Python 3).")
        sys.exit(1)

    parser = argparse.ArgumentParser(description="Debug Cloud SQL instance details.")
    parser.add_argument("resource_name", help="Name of the Cloud SQL instance (or full resource path).")
    parser.add_argument("--project", "-p", help="Project ID where the Cloud SQL instance resides. Defaults to GOOGLE_CLOUD_PROJECT env var.")
    
    args = parser.parse_args()
    
    # Check for googleapiclient
    try:
        from googleapiclient import discovery
    except ImportError:
        print("Error: 'googleapiclient' module not found.")
        print("Please run: pip install google-api-python-client")
        sys.exit(1)

    target_project = args.project or os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    if not target_project:
        # If resource_name has project, we might be fine, but warn if not.
        if '/' not in args.resource_name:
             print("Error: Project ID must be specified via --project or GOOGLE_CLOUD_PROJECT env var.")
             sys.exit(1)
             
    debug_cloudsql(target_project, args.resource_name)
