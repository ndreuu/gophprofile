package domain

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/google/uuid"
)

func ValidateUserID(id string) error {
	if id == "" || len(id) > 255 || strings.TrimSpace(id) != id {
		return fmt.Errorf("%w: X-User-ID must contain 1–255 characters without surrounding spaces", ErrInvalid)
	}
	for _, c := range id {
		if unicode.IsControl(c) {
			return fmt.Errorf("%w: user ID contains control characters", ErrInvalid)
		}
	}
	return nil
}

func ValidateAvatarID(id string) error {
	if _, err := uuid.Parse(id); err != nil {
		return fmt.Errorf("%w: invalid avatar ID", ErrInvalid)
	}
	return nil
}

func ValidateEvent(e Event) error {
	if _, err := uuid.Parse(e.ID); err != nil {
		return fmt.Errorf("%w: invalid message ID", ErrInvalid)
	}
	if err := ValidateAvatarID(e.AvatarID); err != nil {
		return err
	}
	if e.Type != EventUploaded && e.Type != EventDeleted {
		return fmt.Errorf("%w: unknown event type", ErrInvalid)
	}
	if err := ValidateUserID(e.UserID); err != nil {
		return err
	}
	if e.CreatedAt.IsZero() {
		return fmt.Errorf("%w: missing event timestamp", ErrInvalid)
	}
	return nil
}
