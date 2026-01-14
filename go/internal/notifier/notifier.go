// Package notifier provides notification capabilities.
package notifier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/smtp"
	"os"
	"strings"

	"cloud.google.com/go/pubsub"
	"context"
	"github.com/jeffoconnorau/gcbdr_monitor/go/internal/analyzer"
)

// Notifier is the interface for all notification channels.
type Notifier interface {
	Send(anomalies []analyzer.Anomaly) error
}

// Manager orchestrates all configured notifiers.
type Manager struct {
	notifiers []Notifier
}

// NewManager creates a Manager with all configured notifiers.
func NewManager(projectID string) *Manager {
	m := &Manager{}

	// Google Chat
	if webhook := os.Getenv("GOOGLE_CHAT_WEBHOOK"); webhook != "" {
		m.notifiers = append(m.notifiers, &ChatNotifier{WebhookURL: webhook})
		log.Printf("Enabled Google Chat notifications")
	}

	// Email
	if host := os.Getenv("SMTP_HOST"); host != "" {
		m.notifiers = append(m.notifiers, &EmailNotifier{
			Host:       host,
			Port:       getEnvOrDefault("SMTP_PORT", "587"),
			User:       os.Getenv("SMTP_USER"),
			Password:   os.Getenv("SMTP_PASSWORD"),
			Sender:     os.Getenv("EMAIL_SENDER"),
			Recipients: strings.Split(os.Getenv("EMAIL_RECIPIENTS"), ","),
		})
		log.Printf("Enabled Email notifications")
	}

	// Pub/Sub
	if topic := os.Getenv("PUBSUB_TOPIC"); topic != "" {
		m.notifiers = append(m.notifiers, &PubSubNotifier{
			ProjectID: projectID,
			TopicName: topic,
		})
		log.Printf("Enabled Pub/Sub notifications")
	}

	return m
}

// SendNotifications sends anomalies to all configured notifiers.
func (m *Manager) SendNotifications(anomalies []analyzer.Anomaly) {
	if len(anomalies) == 0 {
		return
	}

	for _, n := range m.notifiers {
		if err := n.Send(anomalies); err != nil {
			log.Printf("Notification error: %v", err)
		}
	}
}

// ChatNotifier sends notifications to Google Chat via webhook.
type ChatNotifier struct {
	WebhookURL string
}

// Send sends anomalies to Google Chat.
func (c *ChatNotifier) Send(anomalies []analyzer.Anomaly) error {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("🚨 *GCBDR Alert: %d Anomalies Detected*\n\n", len(anomalies)))

	for i, a := range anomalies {
		if i >= 5 {
			sb.WriteString(fmt.Sprintf("\n... and %d more", len(anomalies)-5))
			break
		}
		sb.WriteString(fmt.Sprintf("• *%s*\n", a.Resource))
		sb.WriteString(fmt.Sprintf("  Job: `%s` | %s %s\n", a.JobID, a.Date, a.Time))
		sb.WriteString(fmt.Sprintf("  Transferred: %.2f GiB (avg: %.2f)\n", a.GiBTransferred, a.AvgGiB))
		sb.WriteString(fmt.Sprintf("  Reasons: %s\n\n", strings.Join(a.Reasons, ", ")))
	}

	payload := map[string]string{"text": sb.String()}
	body, _ := json.Marshal(payload)

	resp, err := http.Post(c.WebhookURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("chat webhook error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("chat webhook returned %d", resp.StatusCode)
	}

	log.Printf("Sent %d anomalies to Google Chat", len(anomalies))
	return nil
}

// EmailNotifier sends notifications via SMTP.
type EmailNotifier struct {
	Host       string
	Port       string
	User       string
	Password   string
	Sender     string
	Recipients []string
}

// Send sends anomalies via email.
func (e *EmailNotifier) Send(anomalies []analyzer.Anomaly) error {
	subject := fmt.Sprintf("GCBDR Alert: %d Anomalies Detected", len(anomalies))

	var body strings.Builder
	body.WriteString("<html><body>")
	body.WriteString(fmt.Sprintf("<h2>%s</h2>", subject))
	body.WriteString("<table border='1' cellpadding='5'>")
	body.WriteString("<tr><th>Resource</th><th>Job ID</th><th>Date/Time</th><th>Transferred</th><th>Reasons</th></tr>")

	for _, a := range anomalies {
		body.WriteString(fmt.Sprintf(
			"<tr><td>%s</td><td>%s</td><td>%s %s</td><td>%.2f GiB</td><td>%s</td></tr>",
			a.Resource, a.JobID, a.Date, a.Time, a.GiBTransferred, strings.Join(a.Reasons, ", "),
		))
	}
	body.WriteString("</table></body></html>")

	msg := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n%s",
		e.Sender,
		strings.Join(e.Recipients, ","),
		subject,
		body.String(),
	)

	auth := smtp.PlainAuth("", e.User, e.Password, e.Host)
	addr := fmt.Sprintf("%s:%s", e.Host, e.Port)

	if err := smtp.SendMail(addr, auth, e.Sender, e.Recipients, []byte(msg)); err != nil {
		return fmt.Errorf("email send error: %w", err)
	}

	log.Printf("Sent %d anomalies via email to %d recipients", len(anomalies), len(e.Recipients))
	return nil
}

// PubSubNotifier publishes anomalies to a Pub/Sub topic.
type PubSubNotifier struct {
	ProjectID string
	TopicName string
}

// Send publishes anomalies to Pub/Sub.
func (p *PubSubNotifier) Send(anomalies []analyzer.Anomaly) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, p.ProjectID)
	if err != nil {
		return fmt.Errorf("pubsub client error: %w", err)
	}
	defer client.Close()

	topic := client.Topic(p.TopicName)

	for _, a := range anomalies {
		data, _ := json.Marshal(a)
		result := topic.Publish(ctx, &pubsub.Message{Data: data})
		if _, err := result.Get(ctx); err != nil {
			log.Printf("Failed to publish anomaly %s: %v", a.JobID, err)
		}
	}

	log.Printf("Published %d anomalies to Pub/Sub topic %s", len(anomalies), p.TopicName)
	return nil
}

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
