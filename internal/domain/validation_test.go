package domain

import (
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestValidation(t *testing.T) {
	for _, id := range []string{"", " u", "u ", "u\n", string(make([]byte, 256))} {
		if ValidateUserID(id) == nil {
			t.Fatalf("accepted %q", id)
		}
	}
	if err := ValidateUserID("alice@example.com"); err != nil {
		t.Fatal(err)
	}
	if ValidateAvatarID("bad") == nil {
		t.Fatal("invalid UUID")
	}
	if ValidateAvatarID(uuid.NewString()) != nil {
		t.Fatal("valid UUID")
	}
	e := Event{ID: uuid.NewString(), AvatarID: uuid.NewString(), UserID: "u", Type: EventUploaded, CreatedAt: time.Now()}
	if ValidateEvent(e) != nil {
		t.Fatal("valid event")
	}
	for _, modify := range []func(*Event){func(e *Event) { e.ID = "bad" }, func(e *Event) { e.AvatarID = "bad" }, func(e *Event) { e.Type = "wat" }, func(e *Event) { e.UserID = "" }, func(e *Event) { e.CreatedAt = time.Time{} }} {
		bad := e
		modify(&bad)
		if ValidateEvent(bad) == nil {
			t.Fatalf("accepted %+v", bad)
		}
	}
	e.Type = EventDeleted
	if ValidateEvent(e) != nil {
		t.Fatal("valid delete event")
	}
}
