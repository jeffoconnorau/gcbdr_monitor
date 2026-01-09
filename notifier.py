import os
import logging
import json
import urllib.request
import ssl
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from google.cloud import logging as cloud_logging

logger = logging.getLogger(__name__)

class NotifierBase:
    def send(self, anomalies):
        raise NotImplementedError

class GoogleChatNotifier(NotifierBase):
    def __init__(self, config_value):
        self.config_value = config_value
        self.is_webhook = config_value.startswith('http')
        self.space_name = None
        
        if not self.is_webhook:
            # Assume it's a Space ID or Name
            # If it's just ID "AAAA...", convert to "spaces/AAAA..."
            if not config_value.startswith('spaces/'):
                self.space_name = f"spaces/{config_value}"
            else:
                self.space_name = config_value

            try:
                import google.auth
                import google.auth.transport.requests
                self.creds, self.project = google.auth.default(scopes=['https://www.googleapis.com/auth/chat.bot'])
                self.auth_req = google.auth.transport.requests.Request()
            except ImportError:
                logger.error("google-auth not found. Cannot use Chat API mode.")
                self.is_webhook = True # Fallback effectively disables it if not URL

    def send(self, anomalies):
        if not anomalies:
            return

        # Limit to first 10 to avoid hitting limits or spamming
        display_anomalies = anomalies[:10]
        remaining = len(anomalies) - 10
        
        cards = []
        for anomaly in display_anomalies:
            cards.append(self._create_card(anomaly))
            
        if remaining > 0:
            cards.append({
                "header": {
                    "title": f"... and {remaining} more anomalies.",
                    "subtitle": "Check the dashboard for full details."
                }
            })

        message = {
            "cardsV2": [{
                "cardId": "gcbdr-anomaly-card",
                "card": {
                    "header": {
                         "title": "GCBDR Monitor Alert",
                         "subtitle": f"{len(anomalies)} anomalies detected"
                    },
                    "sections": [
                         {"widgets": [{"textParagraph": {"text": "<b>Anomaly Report</b>"}}]} 
                    ]
                }
            }]
        }
        # Note: The v1 Webhook API uses "cards", the v1 REST API prefers "cardsV2" or just "cards" but strict validation varies.
        # For simplicity/compatibility, we will try to reuse the 'cards' format if possible, 
        # but the Chat API often demands 'cardsV2' for apps. 
        # Actually, Webhooks support 'cards' (v1) legacy. Chat API supports 'cardsV2'.
        # Let's stick to v1 'cards' for Webhook and try to adapt for API if needed.
        # ... Wait, if we use the REST API with ADC we are effectively a "Chat App". 
        # Chat Apps usually post 'cardsV2'. 
        # Let's keep the existing payload structure for Webhook (v1 'cards') 
        # and construct a basic 'text' or 'cardsV2' for API to be safe, or just try sending 'cards' to API.
        
        # Re-using the existing 'cards' payload for webhook:
        webhook_payload = {"cards": cards}

        if self.is_webhook:
            self._send_via_webhook(webhook_payload)
        else:
            self._send_via_api(webhook_payload)

    def _send_via_webhook(self, message):
        try:
            req = urllib.request.Request(
                self.config_value, 
                data=json.dumps(message).encode('utf-8'),
                headers={'Content-Type': 'application/json'}
            )
            
            skip_verify = os.environ.get('GCBDR_MONITOR_SKIP_SSL_VERIFY', '').lower() == 'true'
            ctx = ssl.create_default_context()
            if skip_verify:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            
            with urllib.request.urlopen(req, context=ctx) as response:
                logger.info(f"Sent alert to Google Chat (Webhook). Status: {response.status}")
        except Exception as e:
            logger.error(f"Failed to send to Google Chat (Webhook): {e}")

    def _send_via_api(self, legacy_payload):
        # The REST API using ADC acts as the Service Account (App).
        # It should post to https://chat.googleapis.com/v1/{space_name}/messages
        
        try:
            if not self.creds.valid:
                self.creds.refresh(self.auth_req)
            
            token = self.creds.token
            url = f"https://chat.googleapis.com/v1/{self.space_name}/messages"
            
            # Apps must use cardsV2 or simple text. Legacy 'cards' might be rejected or deprecated.
            # Let's convert our 'cards' to 'cardsV2' wrapper roughly or just use text fall back if complex.
            # Actually, let's just try sending the legacy 'cards' first as 'cards' field still exists in Message resource.
            # If that fails, we might need to migrate card format.
            
            # API expects JSON: { "cards": [...] } or { "text": "..." }
            
            req = urllib.request.Request(
                url,
                data=json.dumps(legacy_payload).encode('utf-8'),
                headers={
                    'Content-Type': 'application/json',
                    'Authorization': f'Bearer {token}'
                }
            )
            
            with urllib.request.urlopen(req) as response:
                 logger.info(f"Sent alert to Google Chat (API). Status: {response.status}")

        except Exception as e:
            logger.error(f"Failed to send to Google Chat (API): {e}")
            logger.debug(f"Payload was: {json.dumps(legacy_payload)}")

    def _create_card(self, anomaly):
        # Determine color based on reasons (simple heuristic)
        # Check reasons for specific keywords
        reasons = anomaly.get('reasons', '')
        
        # Simple widget list
        widgets = [
            {
                "keyValue": {
                    "topLabel": "Resource",
                    "content": anomaly.get('resource', 'Unknown')
                }
            },
            {
                "keyValue": {
                    "topLabel": "Reasons",
                    "content": reasons,
                    "contentMultiline": True
                }
            },
            {
                "keyValue": {
                    "topLabel": "Change Size",
                    "content": f"{anomaly.get('gib_transferred')} GiB (Avg: {anomaly.get('avg_gib')} GiB)"
                }
            },
             {
                "keyValue": {
                    "topLabel": "Date/Time",
                    "content": f"{anomaly.get('date')} {anomaly.get('time')}"
                }
            }
        ]

        if 'Duration' in reasons:
             widgets.append({
                "keyValue": {
                    "topLabel": "Duration",
                    "content": f"{anomaly.get('duration_seconds')}s (Avg: {anomaly.get('avg_duration_seconds'):.1f}s)"
                }
            })

        return {
            "header": {
                "title": "Backup Anomaly Detected",
                "subtitle": anomaly.get('job_id'),
                "imageUrl": "https://fonts.gstatic.com/s/i/short_term/release/googlesymbols/warning/default/48px.svg",
                "imageStyle": "AVATAR"
            },
            "sections": [
                {
                    "widgets": widgets
                }
            ]
        }

class EmailNotifier(NotifierBase):
    def __init__(self, smtp_host, smtp_port, smtp_user, smtp_password, sender, recipients):
        self.smtp_host = smtp_host
        self.smtp_port = int(smtp_port)
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.sender = sender
        self.recipients = [r.strip() for r in recipients.split(',')]

    def send(self, anomalies):
        if not anomalies:
            return

        msg = MIMEMultipart()
        msg['From'] = self.sender
        msg['To'] = ", ".join(self.recipients)
        msg['Subject'] = f"GCBDR Monitor: {len(anomalies)} Anomalies Detected"

        html_content = self._format_html(anomalies)
        msg.attach(MIMEText(html_content, 'html'))

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(msg)
            logger.info(f"Sent email alert to {len(self.recipients)} recipients")
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"SMTP Authentication Failed: {e}")
            logger.error("HINT: If using Gmail/Outlook, ensure you are using an App Password, not your login password.")
            logger.error("HINT: If your password has special characters, ensure they are properly escaped in the environment variable (e.g., use single quotes in shell).")
        except Exception as e:
            logger.error(f"Failed to send email: {e}")

    def _format_html(self, anomalies):
        rows = ""
        for a in anomalies:
            rows += f"""
            <tr style="background-color: #ffebee;">
                <td style="padding: 8px; border: 1px solid #ddd;">{a.get('resource')}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{a.get('reasons')}</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{a.get('gib_transferred')} (Avg: {a.get('avg_gib')})</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{a.get('duration_seconds')} (Avg: {a.get('avg_duration_seconds'):.1f})</td>
                <td style="padding: 8px; border: 1px solid #ddd;">{a.get('date')} {a.get('time')}</td>
            </tr>
            """
        
        return f"""
        <html>
        <body style="font-family: Arial, sans-serif;">
            <h2>Backup Anomalies Detected</h2>
            <p>The following backup jobs have been flagged as anomalous:</p>
            <table style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #d32f2f; color: white;">
                    <th style="padding: 8px; border: 1px solid #ddd;">Resource</th>
                    <th style="padding: 8px; border: 1px solid #ddd;">Reasons</th>
                    <th style="padding: 8px; border: 1px solid #ddd;">Size (GiB)</th>
                    <th style="padding: 8px; border: 1px solid #ddd;">Duration (s)</th>
                    <th style="padding: 8px; border: 1px solid #ddd;">Time</th>
                </tr>
                {rows}
            </table>
        </body>
        </html>
        """

class NotificationManager:
    def __init__(self, project_id=None):
        self.project_id = project_id
        self.notifiers = []
        
        # Initialize Google Chat
        webhook_url = os.environ.get('GOOGLE_CHAT_WEBHOOK')
        if webhook_url:
            self.notifiers.append(GoogleChatNotifier(webhook_url))
        
        # Initialize Email
        smtp_host = os.environ.get('SMTP_HOST')
        smtp_port = os.environ.get('SMTP_PORT', 587)
        smtp_user = os.environ.get('SMTP_USER')
        smtp_password = os.environ.get('SMTP_PASSWORD')
        email_recipients = os.environ.get('EMAIL_RECIPIENTS')
        email_sender = os.environ.get('EMAIL_SENDER')
        
        if smtp_host and smtp_user and smtp_password and email_recipients and email_sender:
            self.notifiers.append(EmailNotifier(
                smtp_host, smtp_port, smtp_user, smtp_password, email_sender, email_recipients
            ))

        # Initialize Pub/Sub
        pubsub_topic = os.environ.get('PUBSUB_TOPIC')
        if pubsub_topic:
            self.notifiers.append(PubSubNotifier(pubsub_topic))

        # Initialize LogNotifier (Always enabled for Cloud Monitoring)
        # You can disable this by setting ENABLE_LOG_ALERT=false
        if os.environ.get('ENABLE_LOG_ALERT', 'true').lower() == 'true':
            self.notifiers.append(LogNotifier(project_id))

    def send_notifications(self, anomalies):
        if not anomalies:
            return

        for notifier in self.notifiers:
            try:
                notifier.send(anomalies)
            except Exception as e:
                logger.error(f"Notifier failed: {e}")

class PubSubNotifier(NotifierBase):
    def __init__(self, topic_name):
        self.topic_name = topic_name
        # Lazy import to avoid hard dependency if not used
        from google.cloud import pubsub_v1
        self.publisher = pubsub_v1.PublisherClient()

    def send(self, anomalies):
        if not anomalies:
            return

        message_data = json.dumps(anomalies).encode('utf-8')
        
        try:
            future = self.publisher.publish(self.topic_name, message_data)
            message_id = future.result()
            logger.info(f"Published anomalies to Pub/Sub topic {self.topic_name}. Message ID: {message_id}")
        except Exception as e:
            logger.error(f"Failed to publish to Pub/Sub: {e}")

class LogNotifier(NotifierBase):
    def __init__(self, project_id=None):
        self.project_id = project_id

    def send(self, anomalies):
        if not anomalies:
            return

        # Helper to create a short summary string for labels
        # e.g. "db-1 (Size Spike), vm-2 (Duration)"
        summary_items = []
        for a in anomalies[:5]: # Limit to first 5 to keep label size reasonable
            res = a.get('resource', 'unknown')
            reasons = a.get('reasons', '').split(',')[0] # Take first reason only
            if 'Size Spike' in reasons: reasons = 'Size Spike'
            elif 'Size Drop' in reasons: reasons = 'Size Drop'
            elif 'Duration' in reasons: reasons = 'Duration'
            summary_items.append(f"{res} ({reasons})")
        
        if len(anomalies) > 5:
            summary_items.append(f"... +{len(anomalies)-5} more")
            
        summary_text = ", ".join(summary_items)

        # Structured log entry for Cloud Monitoring
        log_entry = {
            "severity": "WARNING",
            "event": "GCBDR_ANOMALY_DETECTED",
            "anomalies_count": len(anomalies),
            "anomalies": anomalies,
            "summary_text": summary_text
        }
        
        # Method 1: Explicitly write to Cloud Logging API (Works locally and in Cloud Run)
        try:
            # Use provided project_id or auto-discover
            client = cloud_logging.Client(project=self.project_id)
            # Use a specific log name to ensure isolation and easy filtering
            logger_client = client.logger("gcbdr_monitor_alerts")
            
            # log_struct sends the JSON payload with correctly set severity
            logger_client.log_struct(
                log_entry, 
                severity='WARNING'
            )
            logger.info(f"Successfully wrote structured log to 'gcbdr_monitor_alerts' for {len(anomalies)} anomalies.")
            
        except Exception as e:
            logger.error(f"Failed to write to Cloud Logging API: {e}")
            logger.info("Falling back to stdout printing...")
            
            # Method 2: Fallback to stdout (Reliable in Cloud Run, but maybe not local)
            try:
                print(json.dumps(log_entry, default=str), flush=True)
            except Exception as e2:
                logger.error(f"Failed to serialize structured log (fallback): {e2}")
                logger.warning(f"GCBDR_ANOMALY_DETECTED (Fallback): Found {len(anomalies)} anomalies. Check logs for serialization errors.")
