import os
import logging
import json
import urllib.request
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

logger = logging.getLogger(__name__)

class NotifierBase:
    def send(self, anomalies):
        raise NotImplementedError

class GoogleChatNotifier(NotifierBase):
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url

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
            "cards": cards
        }
        
        try:
            req = urllib.request.Request(
                self.webhook_url, 
                data=json.dumps(message).encode('utf-8'),
                headers={'Content-Type': 'application/json'}
            )
            with urllib.request.urlopen(req) as response:
                logger.info(f"Sent {len(anomalies)} anomalies to Google Chat. Status: {response.status}")
        except Exception as e:
            logger.error(f"Failed to send to Google Chat: {e}")

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
    def __init__(self):
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

    def send_notifications(self, anomalies):
        if not anomalies:
            return

        for notifier in self.notifiers:
            try:
                notifier.send(anomalies)
            except Exception as e:
                logger.error(f"Notifier failed: {e}")
