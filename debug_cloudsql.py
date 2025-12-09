
import os
import sys
import logging
from googleapiclient import discovery
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def debug_cloudsql(project_id, resource_name):
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
    if len(sys.argv) < 2:
        print("Usage: python3 debug_cloudsql.py <resource_name> [project_id]")
        sys.exit(1)
        
    res_name = sys.argv[1]
    proj_id = sys.argv[2] if len(sys.argv) > 2 else os.environ.get('GOOGLE_CLOUD_PROJECT')
    
    if not proj_id:
        print("Error: GOOGLE_CLOUD_PROJECT env var not set and project_id not provided.")
        sys.exit(1)
        
    debug_cloudsql(proj_id, res_name)
