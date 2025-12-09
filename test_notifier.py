import unittest
from unittest.mock import patch, MagicMock
from notifier import GoogleChatNotifier, EmailNotifier, NotificationManager
import os
import json

class TestNotifier(unittest.TestCase):
    def test_google_chat_notifier(self):
        with patch('urllib.request.urlopen') as mock_urlopen, \
             patch('urllib.request.Request') as mock_request:
            
            # Setup mock response
            mock_response = MagicMock()
            mock_response.status = 200
            mock_urlopen.return_value.__enter__.return_value = mock_response
            
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
