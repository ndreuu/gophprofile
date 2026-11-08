package imageproc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"testing"

	"gophprofile/internal/domain"
)

func encodedImage(t *testing.T, width, height int, format string) []byte {
	t.Helper()
	source := image.NewNRGBA(image.Rect(0, 0, width, height))
	for y := 0; y < height; y++ {
		for x := 0; x < width; x++ {
			source.Set(x, y, color.NRGBA{R: uint8(x % 255), G: uint8(y % 255), B: 100, A: 255})
		}
	}
	var buffer bytes.Buffer
	var err error
	if format == "jpeg" {
		err = jpeg.Encode(&buffer, source, nil)
	} else {
		err = png.Encode(&buffer, source)
	}
	if err != nil {
		t.Fatal(err)
	}
	return buffer.Bytes()
}

func TestInspectDetectsEncodedFormatAndDimensions(t *testing.T) {
	for _, format := range []string{"png", "jpeg"} {
		t.Run(format, func(t *testing.T) {
			info, err := New().Inspect(encodedImage(t, 120, 80, format))
			if err != nil {
				t.Fatal(err)
			}
			if info.MIMEType != "image/"+format || info.Dimensions.Width != 120 || info.Dimensions.Height != 80 {
				t.Fatalf("unexpected image info: %+v", info)
			}
		})
	}
}

func TestInspectRejectsInvalidFiles(t *testing.T) {
	for _, tc := range []struct {
		name string
		data []byte
		want error
	}{
		{"empty", nil, domain.ErrInvalid},
		{"text", []byte("this is not an image"), domain.ErrUnsupported},
		{"too large", make([]byte, domain.MaxFileSize+1), domain.ErrTooLarge},
		{"truncated png", []byte("\x89PNG\r\n\x1a\n"), domain.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New().Inspect(tc.data)
			if !errors.Is(err, tc.want) {
				t.Fatalf("want %v, got %v", tc.want, err)
			}
		})
	}
}

func TestInspectRejectsUnsafeDimensionsBeforePixelDecode(t *testing.T) {
	for _, dimensions := range [][2]uint32{{MaxDimension + 1, 1}, {1, MaxDimension + 1}, {10000, 10000}} {
		data := encodedImage(t, 1, 1, "png")
		binary.BigEndian.PutUint32(data[16:20], dimensions[0])
		binary.BigEndian.PutUint32(data[20:24], dimensions[1])
		binary.BigEndian.PutUint32(data[29:33], crc32.ChecksumIEEE(data[12:29]))
		if _, err := New().Inspect(data); !errors.Is(err, domain.ErrInvalid) {
			t.Fatalf("%v: want invalid image, got %v", dimensions, err)
		}
	}
}

func TestThumbnailsAreDeterministicSquareJPEG(t *testing.T) {
	data := encodedImage(t, 600, 200, "png")
	first, err := New().Thumbnails(data)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New().Thumbnails(data)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("got %d variants", len(first))
	}
	for i, size := range []int{100, 300} {
		variant := first[i]
		decoded, format, err := image.Decode(bytes.NewReader(variant.Data))
		if err != nil {
			t.Fatal(err)
		}
		if decoded.Bounds().Dx() != size || decoded.Bounds().Dy() != size || format != "jpeg" || variant.MIMEType != "image/jpeg" {
			t.Fatalf("unexpected variant %+v / %v / %s", variant.Size, decoded.Bounds(), format)
		}
		if !bytes.Equal(variant.Data, second[i].Data) {
			t.Fatal("reprocessing must produce identical bytes")
		}
	}
}

func TestThumbnailsUseCenterCrop(t *testing.T) {
	source := image.NewRGBA(image.Rect(0, 0, 900, 300))
	for y := 0; y < 300; y++ {
		for x := 0; x < 900; x++ {
			c := color.RGBA{R: 255, A: 255}
			if x >= 300 && x < 600 {
				c = color.RGBA{G: 255, A: 255}
			}
			source.Set(x, y, c)
		}
	}
	var input bytes.Buffer
	if err := png.Encode(&input, source); err != nil {
		t.Fatal(err)
	}
	variants, err := New().Thumbnails(input.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	thumb, err := jpeg.Decode(bytes.NewReader(variants[0].Data))
	if err != nil {
		t.Fatal(err)
	}
	r, g, _, _ := thumb.At(50, 50).RGBA()
	if g < 60000 || r > 1000 {
		t.Fatalf("expected green center crop; got r=%d g=%d", r, g)
	}
}

func TestThumbnailsRejectTruncatedPixelData(t *testing.T) {
	data := encodedImage(t, 50, 50, "png")
	data = data[:33]
	if _, err := New().Inspect(data); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("want invalid pixels during upload validation, got %v", err)
	}
	if _, err := New().Thumbnails(data); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("want invalid pixels, got %v", err)
	}
}

func TestConvertEncodesAllThreeFormats(t *testing.T) {
	data := encodedImage(t, 71, 53, "png")
	for _, format := range []string{"jpeg", "png", "webp"} {
		t.Run(format, func(t *testing.T) {
			converted, mime, err := New().Convert(data, format)
			if err != nil {
				t.Fatal(err)
			}
			image, actual, err := image.Decode(bytes.NewReader(converted))
			if err != nil {
				t.Fatal(err)
			}
			if actual != format || mime != "image/"+format || image.Bounds().Dx() != 71 || image.Bounds().Dy() != 53 {
				t.Fatalf("actual=%s mime=%s bounds=%v", actual, mime, image.Bounds())
			}
			if _, err := New().Inspect(converted); err != nil {
				t.Fatalf("output must be valid upload: %v", err)
			}
			variants, err := New().Thumbnails(converted)
			if err != nil || len(variants) != 2 {
				t.Fatalf("output must be valid thumbnail source: variants=%d err=%v", len(variants), err)
			}
		})
	}
}

func TestConvertPreservesAlphaForPNGAndWebPAndCompositesJPEG(t *testing.T) {
	source := image.NewNRGBA(image.Rect(0, 0, 20, 20))
	source.SetNRGBA(10, 10, color.NRGBA{R: 255, A: 128})
	var encoded bytes.Buffer
	if err := png.Encode(&encoded, source); err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"jpeg", "png", "webp"} {
		t.Run(format, func(t *testing.T) {
			converted, _, err := New().Convert(encoded.Bytes(), format)
			if err != nil {
				t.Fatal(err)
			}
			result, _, err := image.Decode(bytes.NewReader(converted))
			if err != nil {
				t.Fatal(err)
			}
			r, g, b, a := result.At(0, 0).RGBA()
			if format == "jpeg" {
				if r < 60000 || g < 60000 || b < 60000 || a != 65535 {
					t.Fatalf("want opaque white background, got %d %d %d %d", r, g, b, a)
				}
			} else if a != 0 {
				t.Fatalf("want transparent background, got alpha=%d", a)
			}
		})
	}
}

func TestConvertRejectsInvalidFormatOrInput(t *testing.T) {
	if _, _, err := New().Convert(encodedImage(t, 2, 2, "png"), "gif"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	if _, _, err := New().Convert(nil, "png"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
	data := encodedImage(t, 2, 2, "png")[:33]
	if _, _, err := New().Convert(data, "webp"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatal(err)
	}
}
