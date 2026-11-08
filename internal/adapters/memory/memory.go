package memory

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"gophprofile/internal/domain"
)

type entry struct {
	event domain.Event
	sent  bool
}
type Repository struct {
	mu      sync.Mutex
	avatars map[string]domain.Avatar
	inbox   map[string]bool
	outbox  []entry
}

func NewRepository() *Repository {
	return &Repository{avatars: map[string]domain.Avatar{}, inbox: map[string]bool{}}
}
func clone(a domain.Avatar) domain.Avatar {
	a.Thumbnails = append([]domain.Thumbnail{}, a.Thumbnails...)
	if a.DeletedAt != nil {
		d := *a.DeletedAt
		a.DeletedAt = &d
	}
	return a
}
func (r *Repository) Create(ctx context.Context, a domain.Avatar, e domain.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.avatars[a.ID]; exists {
		return fmt.Errorf("avatar already exists")
	}
	r.avatars[a.ID] = clone(a)
	r.outbox = append(r.outbox, entry{event: e})
	return nil
}
func (r *Repository) Get(ctx context.Context, id string) (domain.Avatar, error) {
	if err := ctx.Err(); err != nil {
		return domain.Avatar{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.avatars[id]
	if !ok || a.DeletedAt != nil {
		return domain.Avatar{}, domain.ErrNotFound
	}
	return clone(a), nil
}
func (r *Repository) List(ctx context.Context, user string) ([]domain.Avatar, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	list := []domain.Avatar{}
	for _, a := range r.avatars {
		if a.UserID == user && a.DeletedAt == nil {
			list = append(list, clone(a))
		}
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].CreatedAt.Equal(list[j].CreatedAt) {
			return list[i].ID > list[j].ID
		}
		return list[i].CreatedAt.After(list[j].CreatedAt)
	})
	return list, nil
}
func (r *Repository) Current(ctx context.Context, user string) (domain.Avatar, error) {
	list, err := r.List(ctx, user)
	if err != nil {
		return domain.Avatar{}, err
	}
	if len(list) == 0 {
		return domain.Avatar{}, domain.ErrNotFound
	}
	return list[0], nil
}
func (r *Repository) SoftDelete(ctx context.Context, id, user string, e domain.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.avatars[id]
	if !ok {
		return domain.ErrNotFound
	}
	if a.UserID != user {
		return domain.ErrForbidden
	}
	if a.DeletedAt != nil {
		return nil
	}
	now := time.Now().UTC()
	a.DeletedAt = &now
	a.UpdatedAt = now
	r.avatars[id] = clone(a)
	e.S3Keys = []string{a.S3Key}
	for _, t := range a.Thumbnails {
		e.S3Keys = append(e.S3Keys, t.S3Key)
	}
	r.outbox = append(r.outbox, entry{event: e})
	return nil
}
func (r *Repository) ProcessEvent(ctx context.Context, e domain.Event, fn domain.EventHandler) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inbox[e.ID] {
		return nil
	}
	a, ok := r.avatars[e.AvatarID]
	if !ok {
		return domain.ErrNotFound
	}
	if e.UserID != a.UserID {
		return fmt.Errorf("%w: event owner does not match", domain.ErrInvalid)
	}
	if e.Type == domain.EventUploaded && (a.DeletedAt != nil || a.Status == domain.StatusCompleted) {
		r.inbox[e.ID] = true
		return nil
	}
	if e.Type == domain.EventDeleted && a.DeletedAt == nil {
		return fmt.Errorf("%w: delete event for active avatar", domain.ErrInvalid)
	}
	result, err := fn(ctx, clone(a))
	if err != nil {
		return err
	}
	if e.Type == domain.EventUploaded {
		a.Thumbnails = result.Thumbnails
		a.Status = result.Status
		a.UpdatedAt = time.Now().UTC()
		r.avatars[a.ID] = clone(a)
	}
	r.inbox[e.ID] = true
	return nil
}
func (r *Repository) DispatchPending(ctx context.Context, publish func(context.Context, domain.Event) error) (bool, error) {
	r.mu.Lock()
	idx := -1
	var e domain.Event
	for i, item := range r.outbox {
		if !item.sent {
			idx = i
			e = item.event
			break
		}
	}
	r.mu.Unlock()
	if idx < 0 {
		return false, nil
	}
	if err := publish(ctx, e); err != nil {
		return false, err
	}
	r.mu.Lock()
	r.outbox[idx].sent = true
	r.mu.Unlock()
	return true, nil
}
func (r *Repository) Ping(ctx context.Context) error { return ctx.Err() }

type Storage struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func NewStorage() *Storage { return &Storage{objects: map[string][]byte{}} }
func (s *Storage) Put(ctx context.Context, key string, reader io.Reader, size int64, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	if int64(len(data)) != size {
		return fmt.Errorf("object size mismatch")
	}
	s.mu.Lock()
	s.objects[key] = bytes.Clone(data)
	s.mu.Unlock()
	return nil
}
func (s *Storage) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	data, ok := s.objects[key]
	if !ok {
		return nil, domain.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(data))), nil
}
func (s *Storage) Delete(ctx context.Context, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.objects, key)
	s.mu.Unlock()
	return nil
}
func (s *Storage) Ping(ctx context.Context) error { return ctx.Err() }

type Bus struct{ queue chan domain.Event }

func NewBus() *Bus { return &Bus{queue: make(chan domain.Event, 256)} }
func (b *Bus) Publish(ctx context.Context, e domain.Event) error {
	select {
	case b.queue <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (b *Bus) Ping(ctx context.Context) error { return ctx.Err() }
func (b *Bus) Run(ctx context.Context, handler func(context.Context, domain.Event) error) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-b.queue:
			for {
				if err := handler(ctx, e); err == nil {
					break
				}
				timer := time.NewTimer(time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				case <-timer.C:
				}
			}
		}
	}
}
