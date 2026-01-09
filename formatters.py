import io
import csv

def format_csv(results):
    """
    Formats the results as a CSV string.
    """
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Header
    writer.writerow(['Resource Name', 'Type', 'Source', 'Total Size (GiB)', 'Daily Change (GB)', 'Daily Change (%)', 'Job Count'])
    
    # Data
    # Combine vault and appliance stats
    all_stats = results.get('vault_workloads', {}).get('resource_stats', []) + \
                results.get('appliance_workloads', {}).get('resource_stats', [])
                
    for r in all_stats:
        writer.writerow([
            r.get('resource_name'),
            r.get('resource_type'),
            r.get('job_source'),
            r.get('total_resource_size_gb'),
            r.get('current_daily_change_gb'),
            r.get('current_daily_change_pct'),
            r.get('backup_job_count')
        ])
    
    # Anomalies Section
    anomalies = results.get('anomalies', [])
    if anomalies:
        writer.writerow([])
        writer.writerow(['ANOMALIES DETECTED'])
        writer.writerow(['Job ID', 'Resource', 'Date', 'Time', 'Change (GB)', 'Avg (GB)', 'Duration (s)', 'Avg Duration (s)', 'Reasons'])
        for a in anomalies:
            writer.writerow([
                a.get('job_id'),
                a.get('resource'),
                a.get('date'),
                a.get('time'),
                a.get('gib_transferred'),
                a.get('avg_gib'),
                a.get('duration_seconds'),
                f"{a.get('avg_duration_seconds', 0):.1f}",
                a.get('reasons')
            ])
        
    return output.getvalue()
