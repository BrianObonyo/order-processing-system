package services

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
)

type PayPalGateway struct {
	baseURL    string
	clientID   string
	secret     string
	httpClient *http.Client

	mu       sync.RWMutex
	captures map[string]string
	statuses map[string]payment.ChargeStatus
}

func NewPayPalGateway(baseURL, clientID, secret string) (payment.Gateway, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = "https://api-m.sandbox.paypal.com"
	}
	if strings.TrimSpace(clientID) == "" || strings.TrimSpace(secret) == "" {
		return nil, fmt.Errorf("PayPal client ID and secret are required")
	}
	return &PayPalGateway{
		baseURL: baseURL, clientID: clientID, secret: secret,
		httpClient: &http.Client{Timeout: 15 * time.Second},
		captures:   make(map[string]string), statuses: make(map[string]payment.ChargeStatus),
	}, nil
}

func (g *PayPalGateway) Charge(ctx context.Context, idempotencyKey string, amountCents int64) error {
	token, err := g.accessToken(ctx)
	if err != nil {
		return err
	}
	g.mu.RLock()
	status, exists := g.statuses[idempotencyKey]
	g.mu.RUnlock()
	if exists {
		switch status {
		case payment.ChargeStatusSucceeded:
			return nil
		case payment.ChargeStatusDeclined:
			return payment.ErrDeclined
		case payment.ChargeStatusPending:
			return payment.ErrTransient
		}
	}

	body := map[string]any{
		"intent": "CAPTURE",
		"purchase_units": []map[string]any{{
			"reference_id": idempotencyKey,
			"custom_id":    idempotencyKey,
			"amount": map[string]string{
				"currency_code": "USD",
				"value":         fmt.Sprintf("%.2f", float64(amountCents)/100),
			},
		}},
	}
	var order struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}
	response, err := g.do(ctx, http.MethodPost, "/v2/checkout/orders", token, idempotencyKey, body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 500 {
		g.mu.Lock()
		g.statuses[idempotencyKey] = payment.ChargeStatusPending
		g.mu.Unlock()
		return payment.ErrTransient
	}
	if response.StatusCode != http.StatusCreated {
		return g.payPalError(response)
	}
	if err := json.NewDecoder(response.Body).Decode(&order); err != nil {
		return fmt.Errorf("decode PayPal order: %w", err)
	}

	captureBody := map[string]any{}
	var captured struct {
		Status        string `json:"status"`
		PurchaseUnits []struct {
			Payments struct {
				Captures []struct {
					ID string `json:"id"`
				} `json:"captures"`
			} `json:"payments"`
		} `json:"purchase_units"`
	}
	captureResponse, err := g.do(ctx, http.MethodPost, "/v2/checkout/orders/"+url.PathEscape(order.ID)+"/capture", token, idempotencyKey, captureBody)
	if err != nil {
		return err
	}
	defer captureResponse.Body.Close()
	if captureResponse.StatusCode >= 500 {
		g.mu.Lock()
		g.statuses[idempotencyKey] = payment.ChargeStatusPending
		g.mu.Unlock()
		return payment.ErrTransient
	}
	if captureResponse.StatusCode != http.StatusCreated {
		if captureResponse.StatusCode == http.StatusUnprocessableEntity || captureResponse.StatusCode == http.StatusBadRequest {
			g.mu.Lock()
			g.statuses[idempotencyKey] = payment.ChargeStatusDeclined
			g.mu.Unlock()
			return payment.ErrDeclined
		}
		return g.payPalError(captureResponse)
	}
	if err := json.NewDecoder(captureResponse.Body).Decode(&captured); err != nil {
		return fmt.Errorf("decode PayPal capture: %w", err)
	}
	captureID := ""
	if len(captured.PurchaseUnits) > 0 && len(captured.PurchaseUnits[0].Payments.Captures) > 0 {
		captureID = captured.PurchaseUnits[0].Payments.Captures[0].ID
	}
	if captured.Status != "COMPLETED" || captureID == "" {
		g.mu.Lock()
		g.statuses[idempotencyKey] = payment.ChargeStatusPending
		g.mu.Unlock()
		return payment.ErrTransient
	}
	g.mu.Lock()
	g.captures[idempotencyKey] = captureID
	g.statuses[idempotencyKey] = payment.ChargeStatusSucceeded
	g.mu.Unlock()
	return nil
}

func (g *PayPalGateway) Refund(ctx context.Context, paymentID string, amountCents int64) error {
	g.mu.RLock()
	captureID := g.captures[paymentID]
	status := g.statuses[paymentID]
	g.mu.RUnlock()
	if status != payment.ChargeStatusSucceeded || captureID == "" {
		return payment.ErrNotFound
	}
	token, err := g.accessToken(ctx)
	if err != nil {
		return err
	}
	body := map[string]any{"amount": map[string]string{
		"currency_code": "USD",
		"value":         fmt.Sprintf("%.2f", float64(amountCents)/100),
	}}
	response, err := g.do(ctx, http.MethodPost, "/v2/payments/captures/"+url.PathEscape(captureID)+"/refund", token, paymentID, body)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 500 {
		return payment.ErrTransient
	}
	if response.StatusCode == http.StatusCreated || response.StatusCode == http.StatusOK {
		return nil
	}
	if response.StatusCode == http.StatusNotFound {
		return payment.ErrNotFound
	}
	return g.payPalError(response)
}

func (g *PayPalGateway) GetCharge(ctx context.Context, idempotencyKey string) (payment.ChargeStatus, error) {
	g.mu.RLock()
	status, exists := g.statuses[idempotencyKey]
	g.mu.RUnlock()
	if exists {
		return status, nil
	}
	return payment.ChargeStatusNotFound, nil
}

func (g *PayPalGateway) accessToken(ctx context.Context) (string, error) {
	form := url.Values{"grant_type": {"client_credentials"}}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, g.baseURL+"/v1/oauth2/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.SetBasicAuth(g.clientID, g.secret)
	response, err := g.httpClient.Do(request)
	if err != nil {
		return "", fmt.Errorf("PayPal authentication: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 500 {
		return "", payment.ErrTransient
	}
	if response.StatusCode != http.StatusOK {
		return "", g.payPalError(response)
	}
	var result struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode PayPal access token: %w", err)
	}
	if result.AccessToken == "" {
		return "", fmt.Errorf("PayPal authentication returned no access token")
	}
	return result.AccessToken, nil
}

func (g *PayPalGateway) do(ctx context.Context, method, path, token, idempotencyKey string, body any) (*http.Response, error) {
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	request, err := http.NewRequestWithContext(ctx, method, g.baseURL+path, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("PayPal-Request-Id", base64.RawURLEncoding.EncodeToString([]byte(idempotencyKey)))
	response, err := g.httpClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("PayPal request: %w", err)
	}
	return response, nil
}

func (g *PayPalGateway) payPalError(response *http.Response) error {
	defer response.Body.Close()
	payload, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	message := strings.TrimSpace(string(payload))
	if message == "" {
		message = response.Status
	}
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return fmt.Errorf("PayPal authorization failed: %s", message)
	}
	if response.StatusCode == http.StatusTooManyRequests {
		return payment.ErrTransient
	}
	return fmt.Errorf("PayPal API returned %d: %s", response.StatusCode, message)
}
