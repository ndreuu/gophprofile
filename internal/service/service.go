package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	"gophprofile/internal/domain"
)

type Service struct {
	repo      domain.Repository
	storage   domain.Storage
	processor domain.ImageProcessor
}

func New(repo domain.Repository, storage domain.Storage, processor domain.ImageProcessor) *Service {
	return &Service{repo: repo, storage: storage, processor: processor}
}

func (s *Service) Upload(ctx context.Context, userID, fileName string, data []byte) (domain.Avatar, error) {
	if err := domain.ValidateUserID(userID); err != nil {
		return domain.Avatar{}, err
	}
	if int64(len(data)) > domain.MaxFileSize {
		return domain.Avatar{}, domain.ErrTooLarge
	}
	if len(data) == 0 {
		return domain.Avatar{}, fmt.Errorf("%w: empty file", domain.ErrInvalid)
	}
	info, err := s.processor.Inspect(data)
	if err != nil {
		return domain.Avatar{}, err
	}
	id := uuid.NewString()
	now := time.Now().UTC()
	fileName = path.Base(strings.ReplaceAll(fileName, "\\", "/"))
	if fileName == "." || fileName == "/" || len(fileName) > 255 {
		fileName = "avatar"
	}
	avatar := domain.Avatar{ID: id, UserID: userID, FileName: fileName, MIMEType: info.MIMEType,
		Size: int64(len(data)), S3Key: "originals/" + id, Dimensions: info.Dimensions,
		Thumbnails: []domain.Thumbnail{}, Status: domain.StatusProcessing,
		URL: "/api/v1/avatars/" + id, CreatedAt: now, UpdatedAt: now}
	if err := s.storage.Put(ctx, avatar.S3Key, bytes.NewReader(data), avatar.Size, avatar.MIMEType); err != nil {
		return domain.Avatar{}, fmt.Errorf("store original: %w", err)
	}
	event := domain.Event{ID: uuid.NewString(), Type: domain.EventUploaded, AvatarID: id,
		UserID: userID, S3Key: avatar.S3Key, CreatedAt: now}
	if err := s.repo.Create(ctx, avatar, event); err != nil {

		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()

		_, lookupErr := s.repo.Get(cleanupCtx, id)
		if !errors.Is(lookupErr, domain.ErrNotFound) {
			return domain.Avatar{}, fmt.Errorf("save avatar (original retained because commit outcome is uncertain): %w", err)
		}
		if cleanupErr := s.storage.Delete(cleanupCtx, avatar.S3Key); cleanupErr != nil {
			return domain.Avatar{}, errors.Join(fmt.Errorf("save avatar: %w", err), fmt.Errorf("cleanup original: %w", cleanupErr))
		}
		return domain.Avatar{}, fmt.Errorf("save avatar: %w", err)
	}
	return avatar, nil
}

func (s *Service) Get(ctx context.Context, id string) (domain.Avatar, error) {
	if err := domain.ValidateAvatarID(id); err != nil {
		return domain.Avatar{}, err
	}
	return s.repo.Get(ctx, id)
}
func (s *Service) Current(ctx context.Context, userID string) (domain.Avatar, error) {
	if err := domain.ValidateUserID(userID); err != nil {
		return domain.Avatar{}, err
	}
	return s.repo.Current(ctx, userID)
}
func (s *Service) List(ctx context.Context, userID string) ([]domain.Avatar, error) {
	if err := domain.ValidateUserID(userID); err != nil {
		return nil, err
	}
	return s.repo.List(ctx, userID)
}
func (s *Service) Delete(ctx context.Context, id, userID string) error {
	if err := domain.ValidateAvatarID(id); err != nil {
		return err
	}
	if err := domain.ValidateUserID(userID); err != nil {
		return err
	}
	return s.repo.SoftDelete(ctx, id, userID, domain.Event{ID: uuid.NewString(), Type: domain.EventDeleted, AvatarID: id, UserID: userID, CreatedAt: time.Now().UTC()})
}
func (s *Service) DeleteCurrent(ctx context.Context, targetUserID, requesterID string) error {
	if err := domain.ValidateUserID(requesterID); err != nil {
		return err
	}
	if targetUserID != requesterID {
		return domain.ErrForbidden
	}
	avatar, err := s.Current(ctx, targetUserID)
	if err != nil {
		return err
	}
	return s.Delete(ctx, avatar.ID, requesterID)
}

// Content returns originals while processing; requested thumbnails become available
// only after completion. Formats are preserved for originals and JPEG for previews.
func (s *Service) Content(ctx context.Context, id, size string) (domain.Avatar, io.ReadCloser, string, error) {
	if size == "" {
		size = "original"
	}
	if size != "original" && size != "100x100" && size != "300x300" {
		return domain.Avatar{}, nil, "", fmt.Errorf("%w: size must be original, 100x100 or 300x300", domain.ErrInvalid)
	}
	avatar, err := s.Get(ctx, id)
	if err != nil {
		return domain.Avatar{}, nil, "", err
	}
	key, mime := avatar.S3Key, avatar.MIMEType
	if size != "original" {
		key = ""
		for _, thumb := range avatar.Thumbnails {
			if thumb.Size == size {
				key = thumb.S3Key
				break
			}
		}
		if key == "" {
			if avatar.Status == domain.StatusProcessing {
				return avatar, nil, "", domain.ErrProcessing
			}
			return avatar, nil, "", domain.ErrNotFound
		}
		mime = "image/jpeg"
	}
	body, err := s.storage.Get(ctx, key)
	if err != nil {
		return avatar, nil, "", fmt.Errorf("read image: %w", err)
	}
	return avatar, body, mime, nil
}

func (s *Service) ContentFormatted(ctx context.Context, id, size, format string) (domain.Avatar, io.ReadCloser, string, error) {
	if format != "" && format != "jpeg" && format != "png" && format != "webp" {
		return domain.Avatar{}, nil, "", fmt.Errorf("%w: format must be jpeg, png or webp", domain.ErrInvalid)
	}
	avatar, body, mime, err := s.Content(ctx, id, size)
	if err != nil {
		return avatar, nil, "", err
	}
	if format == "" {
		return avatar, body, mime, nil
	}
	data, readErr := io.ReadAll(io.LimitReader(body, domain.MaxFileSize+1))
	closeErr := body.Close()
	if readErr != nil {
		return avatar, nil, "", fmt.Errorf("read image for conversion: %w", readErr)
	}
	if closeErr != nil {
		return avatar, nil, "", fmt.Errorf("close image for conversion: %w", closeErr)
	}
	if int64(len(data)) > domain.MaxFileSize {
		return avatar, nil, "", domain.ErrTooLarge
	}
	if mime == "image/"+format {
		avatar.Size = int64(len(data))
		return avatar, io.NopCloser(bytes.NewReader(data)), mime, nil
	}
	converted, contentType, err := s.processor.Convert(data, format)
	if err != nil {
		return avatar, nil, "", err
	}
	avatar.Size = int64(len(converted))
	avatar.MIMEType = contentType
	return avatar, io.NopCloser(bytes.NewReader(converted)), contentType, nil
}
