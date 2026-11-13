package inventory

import (
	"errors"
	"fmt"
)

var ErrUnknownProduct = errors.New("unknown product")

type InsufficientStockError struct {
	ProductID string
	Requested int
}

func (e *InsufficientStockError) Error() string {
	return fmt.Sprintf("insufficient stock for product %s (requested %d)", e.ProductID, e.Requested)
}

type ProductStock struct {
	ProductID  string `json:"product_id"`
	SKU        string `json:"sku"`
	Name       string `json:"name"`
	PriceCents int64  `json:"price_cents"`
	OnHand     int    `json:"on_hand"`
	Reserved   int    `json:"reserved"`
	Available  int    `json:"available"`
}
