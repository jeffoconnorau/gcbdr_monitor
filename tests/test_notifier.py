import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from unittest.mock import MagicMock
# Mock google packages globally before any import
sys.modules['google'] = MagicMock()
sys.modules['google.cloud'] = MagicMock()
sys.modules['google.cloud.pubsub_v1'] = MagicMock()
sys.modules['google.cloud'].pubsub_v1 = sys.modules['google.cloud.pubsub_v1']

import unittest
from unittest.mock import patch
from notifier import GoogleChatNotifier, EmailNotifier, NotificationManager
from notifier import GoogleChatNotifier, EmailNotifier, NotificationManager
import os
import json
import smtplib

class TestNotifier(unittest.TestCase):
    def test_google_chat_notifier(self):
        with patch('urllib.request.urlopen') as mock_urlopen, \
             patch('urllib.request.Request') as mock_request, \
             patch('ssl.create_default_context') as mock_ssl_context:
            
            # Setup mock response
            mock_response = MagicMock()
            mock_response.status = 200
            mock_urlopen.return_value.__enter__.return_value = mock_response
            
            # Setup mock context
            mock_ctx = MagicMock()
            mock_ssl_context.return_value = mock_ctx
            
            notifier = GoogleChatNotifier('http://webhook')
            anomalies = [{
                'job_id': 'job-1',
                'resource': 'vm-1',
                'reasons': 'Size Spike',
                'gib_transferred': 10,
                'avg_gib': 1,
                'date': '2023-01-01',
                'time': '10:00:00',
                'duration_seconds': 100,
                'avg_duration_seconds': 10
            }]
            
            notifier.send(anomalies)
            
            mock_request.assert_called_once()
            args, kwargs = mock_request.call_args
            self.assertEqual(args[0], 'http://webhook')
            
            # Verify context passed
            mock_ssl_context.assert_called_once()
            mock_urlopen.assert_called_once()
            call_args = mock_urlopen.call_args
            self.assertEqual(call_args[1]['context'], mock_ctx)
            
            # Verify data
            # data is bytes, so we need to decode or parse
            data = json.loads(kwargs['data'].decode('utf-8'))
            self.assertEqual(data['cards'][0]['sections'][0]['widgets'][0]['keyValue']['content'], 'vm-1')

    def test_email_notifier(self):
        with patch('smtplib.SMTP') as mock_smtp:
            notifier = EmailNotifier('smtp.host', 587, 'user', 'pass', 'sender@example.com', 'recipient@example.com')
            anomalies = [{
                'resource': 'vm-1', 
                'reasons': 'Size Spike',
                'gib_transferred': 10,
                'avg_gib': 1,
                'duration_seconds': 100,
                'avg_duration_seconds': 10,
                'date': '2023-01-01',
                'time': '10:00:00'
            }]
            
            notifier.send(anomalies)
            
            instance = mock_smtp.return_value.__enter__.return_value
            instance.send_message.assert_called_once()
            
    def test_notification_manager_init(self):
        # Test with no env vars
        with patch.dict(os.environ, {}, clear=True):
            nm = NotificationManager()
            self.assertEqual(len(nm.notifiers), 0)
            
        # Test with Chat env var
        with patch.dict(os.environ, {'GOOGLE_CHAT_WEBHOOK': 'http://webhook'}, clear=True):
            nm = NotificationManager()
            self.assertEqual(len(nm.notifiers), 1)
            self.assertIsInstance(nm.notifiers[0], GoogleChatNotifier)

    def test_email_notifier_auth_error(self):
        with patch('smtplib.SMTP') as mock_smtp, \
             patch('notifier.logger') as mock_logger:
            
            # Setup mock to raise SMTPAuthenticationError
            mock_smtp.return_value.__enter__.return_value.login.side_effect = smtplib.SMTPAuthenticationError(535, b'Authentication failed')
            
            notifier = EmailNotifier('smtp.host', 587, 'user', 'pass', 'sender@example.com', 'recipient@example.com')
            anomalies = [{
                'resource': 'vm-1', 
                'reasons': 'Size Spike',
                'gib_transferred': 10,
                'avg_gib': 1,
                'duration_seconds': 100,
                'avg_duration_seconds': 10,
                'date': '2023-01-01',
                'time': '10:00:00'
            }]
            
            notifier.send(anomalies)
            
            # Verify explicit error logging
            args, _ = mock_logger.error.call_args_list[0]
            self.assertIn("SMTP Authentication Failed", args[0])
            
            # Verify hints are logged
            found_hint = False
            for call in mock_logger.error.call_args_list:
                if "HINT: If using Gmail/Outlook" in call[0][0]:
                    found_hint = True
                    break
            self.assertTrue(found_hint, "App Password hint not logged")

    def test_pubsub_notifier(self):
        from notifier import PubSubNotifier
        
        # Configure mock
        mock_pubsub = sys.modules['google.cloud.pubsub_v1']
        mock_publisher_instance = MagicMock()
        mock_pubsub.PublisherClient.return_value = mock_publisher_instance
        mock_publisher_instance.publish.return_value.result.return_value = "msg-id-123"
        
        notifier = PubSubNotifier("projects/my-project/topics/my-topic")
        anomalies = [{'job_id': 'job-1'}]
        notifier.send(anomalies)
        
        expected_data = json.dumps(anomalies).encode('utf-8')
        mock_publisher_instance.publish.assert_called_once_with("projects/my-project/topics/my-topic", expected_data)

    def test_manager_pubsub_init(self):
        from notifier import PubSubNotifier
        with patch.dict(os.environ, {'PUBSUB_TOPIC': 'projects/p/topics/t'}, clear=True):
            nm = NotificationManager()
            self.assertTrue(any(isinstance(n, PubSubNotifier) for n in nm.notifiers))
