package imageproc

import (
	"bytes"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/jpeg"
	"image/png"

	"github.com/HugoSmits86/nativewebp"
	"github.com/disintegration/imaging"
	_ "golang.org/x/image/webp"

	"gophprofile/internal/domain"
)

const (
	MaxDimension = 16384
	MaxPixels    = 40_000_000
	jpegQuality  = 85
)

type Processor struct{}

func New() *Processor { return &Processor{} }

func inspectHeader(data []byte) (domain.ImageInfo, error) {
	if len(data) == 0 {
		return domain.ImageInfo{}, fmt.Errorf("empty image: %w", domain.ErrInvalid)
	}
	if int64(len(data)) > domain.MaxFileSize {
		return domain.ImageInfo{}, domain.ErrTooLarge
	}
	config, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		if err == image.ErrFormat {
			return domain.ImageInfo{}, fmt.Errorf("decode image header: %w", domain.ErrUnsupported)
		}
		return domain.ImageInfo{}, fmt.Errorf("decode image header: %w: %v", domain.ErrInvalid, err)
	}
	mime, ok := map[string]string{"jpeg": "image/jpeg", "png": "image/png", "webp": "image/webp"}[format]
	if !ok {
		return domain.ImageInfo{}, fmt.Errorf("image format %q: %w", format, domain.ErrUnsupported)
	}
	if config.Width <= 0 || config.Height <= 0 || config.Width > MaxDimension || config.Height > MaxDimension || int64(config.Width)*int64(config.Height) > MaxPixels {
		return domain.ImageInfo{}, fmt.Errorf("image dimensions %dx%d exceed safe limits: %w", config.Width, config.Height, domain.ErrInvalid)
	}
	return domain.ImageInfo{MIMEType: mime, Dimensions: domain.Dimensions{Width: config.Width, Height: config.Height}}, nil
}

func (p *Processor) Inspect(data []byte) (domain.ImageInfo, error) {
	info, err := inspectHeader(data)
	if err != nil {
		return domain.ImageInfo{}, err
	}
	if _, _, err := image.Decode(bytes.NewReader(data)); err != nil {
		return domain.ImageInfo{}, fmt.Errorf("decode image pixels: %w: %v", domain.ErrInvalid, err)
	}
	return info, nil
}

func (p *Processor) Thumbnails(data []byte) ([]domain.Variant, error) {
	if _, err := inspectHeader(data); err != nil {
		return nil, err
	}
	source, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("decode image pixels: %w: %v", domain.ErrInvalid, err)
	}
	variants := make([]domain.Variant, 0, 2)
	for _, size := range []int{100, 300} {
		thumbnail := imaging.Fill(source, size, size, imaging.Center, imaging.Lanczos)

		opaque := image.NewRGBA(thumbnail.Bounds())
		draw.Draw(opaque, opaque.Bounds(), &image.Uniform{C: color.White}, image.Point{}, draw.Src)
		draw.Draw(opaque, opaque.Bounds(), thumbnail, thumbnail.Bounds().Min, draw.Over)
		var buf bytes.Buffer
		if err := jpeg.Encode(&buf, opaque, &jpeg.Options{Quality: jpegQuality}); err != nil {
			return nil, fmt.Errorf("encode %d thumbnail: %w", size, err)
		}
		variants = append(variants, domain.Variant{Size: fmt.Sprintf("%dx%d", size, size), Data: buf.Bytes(), MIMEType: "image/jpeg"})
	}
	return variants, nil
}

var _ domain.ImageProcessor = (*Processor)(nil)

// Convert returns a validated image encoded as JPEG, PNG, or lossless WebP.
func (p *Processor) Convert(data []byte, format string) ([]byte, string, error) {
	mime, ok := map[string]string{"jpeg": "image/jpeg", "png": "image/png", "webp": "image/webp"}[format]
	if !ok {
		return nil, "", fmt.Errorf("%w: format must be jpeg, png or webp", domain.ErrInvalid)
	}
	info, err := inspectHeader(data)
	if err != nil {
		return nil, "", err
	}
	source, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, "", fmt.Errorf("decode conversion source: %w: %v", domain.ErrInvalid, err)
	}
	if mime == info.MIMEType {
		return data, mime, nil
	}
	var output bytes.Buffer
	switch format {
	case "jpeg":
		opaque := image.NewRGBA(source.Bounds())
		draw.Draw(opaque, opaque.Bounds(), &image.Uniform{C: color.White}, image.Point{}, draw.Src)
		draw.Draw(opaque, opaque.Bounds(), source, source.Bounds().Min, draw.Over)
		err = jpeg.Encode(&output, opaque, &jpeg.Options{Quality: jpegQuality})
	case "png":
		err = png.Encode(&output, source)
	case "webp":
		err = nativewebp.Encode(&output, source, &nativewebp.Options{CompressionLevel: nativewebp.BestSpeed})
	}
	if err != nil {
		return nil, "", fmt.Errorf("encode image as %s: %w", format, err)
	}
	return output.Bytes(), mime, nil
}
