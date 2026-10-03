use crate::{MAX_BYTES, MAX_DIMENSION, MAX_SIDE};
use image::{DynamicImage, ImageFormat, ImageReader, Limits};
use std::io::{Cursor, Write};
/// Image failures retain decoder and encoder sources.
#[derive(Debug, thiserror::Error)]
pub enum ImageError {
    /// Application bounds or invalid operation.
    #[error(transparent)]
    Bounds(#[from] cellule_runtime::Error),
    /// PNG decoder or encoder failure.
    #[error(transparent)]
    Codec(#[from] image::ImageError),
}
fn decode(bytes: &[u8]) -> Result<DynamicImage, ImageError> {
    if bytes.is_empty() || bytes.len() > MAX_BYTES {
        return Err(cellule_runtime::Error::Command("PNG must be 1..256 KiB").into());
    }
    let mut limits = Limits::default();
    limits.max_image_width = Some(MAX_DIMENSION);
    limits.max_image_height = Some(MAX_DIMENSION);
    limits.max_alloc = Some(32 << 20);
    let mut reader = ImageReader::with_format(Cursor::new(bytes), ImageFormat::Png);
    reader.limits(limits);
    let image = reader.decode()?;
    if image.width() == 0 || image.height() == 0 {
        return Err(cellule_runtime::Error::Command("empty image").into());
    }
    Ok(image)
}
/// Decodes PNG with strict dimension limits before admitting it as immutable source.
pub fn dimensions(bytes: &[u8]) -> Result<(u32, u32), ImageError> {
    let image = decode(bytes)?;
    Ok((image.width(), image.height()))
}
pub(crate) fn thumbnail_dimensions(
    width: u32,
    height: u32,
    side: u32,
) -> cellule_runtime::Result<(u32, u32)> {
    if width == 0
        || height == 0
        || width > MAX_DIMENSION
        || height > MAX_DIMENSION
        || !(1..=MAX_SIDE).contains(&side)
    {
        return Err(cellule_runtime::Error::Command(
            "invalid thumbnail dimensions",
        ));
    }
    // Integer floor rounding is the definition's compatibility contract. Never upscale.
    let largest = width.max(height);
    if largest <= side {
        return Ok((width, height));
    }
    Ok((
        (u64::from(width) * u64::from(side) / u64::from(largest)).max(1) as u32,
        (u64::from(height) * u64::from(side) / u64::from(largest)).max(1) as u32,
    ))
}
struct BoundedOutput(Vec<u8>);
impl Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(bytes.len()) > MAX_BYTES {
            return Err(std::io::Error::other("encoded PNG exceeds 256 KiB"));
        }
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
/// Converts to RGBA8, preserves aspect ratio, and deterministically encodes a bounded PNG.
/// Embeddings run this CPU operation in an owned blocking task outside SQLite.
pub fn thumbnail(bytes: &[u8], side: u32) -> Result<Vec<u8>, ImageError> {
    let image = decode(bytes)?;
    let (width, height) = thumbnail_dimensions(image.width(), image.height(), side)?;
    let pixels = image::imageops::resize(
        &image.to_rgba8(),
        width,
        height,
        image::imageops::FilterType::Triangle,
    );
    let mut output = BoundedOutput(Vec::new());
    image::ImageEncoder::write_image(
        image::codecs::png::PngEncoder::new(&mut output),
        pixels.as_raw(),
        width,
        height,
        image::ExtendedColorType::Rgba8,
    )?;
    Ok(output.0)
}
/// Small deterministic PNG fixture used by the executable journey.
pub fn sample_png() -> Result<Vec<u8>, ImageError> {
    let pixels = image::RgbaImage::from_fn(320, 160, |x, y| {
        image::Rgba([(x % 251) as u8, (y % 251) as u8, 100, 255])
    });
    let mut output = BoundedOutput(Vec::new());
    image::ImageEncoder::write_image(
        image::codecs::png::PngEncoder::new(&mut output),
        pixels.as_raw(),
        320,
        160,
        image::ExtendedColorType::Rgba8,
    )?;
    Ok(output.0)
}
#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    #[test]
    fn deterministic_bounded_thumbnail() {
        let source = super::sample_png().unwrap();
        let result = super::thumbnail(&source, 128).unwrap();
        assert_eq!(super::dimensions(&result).unwrap(), (128, 64));
        assert_eq!(super::thumbnail(&source, 128).unwrap(), result);
        assert!(result.len() <= crate::MAX_BYTES);
    }
    #[test]
    fn malformed_and_oversized_sources_fail() {
        assert!(super::dimensions(b"not PNG").is_err());
        assert!(super::dimensions(&vec![0; crate::MAX_BYTES + 1]).is_err());
        assert!(super::thumbnail_dimensions(1025, 1, 1).is_err());
        assert!(super::thumbnail_dimensions(10, 10, 129).is_err());
    }
    #[test]
    fn rounding_and_no_upscale() {
        assert_eq!(super::thumbnail_dimensions(3, 1024, 1).unwrap(), (1, 1));
        assert_eq!(super::thumbnail_dimensions(32, 16, 128).unwrap(), (32, 16));
    }
}

#[cfg(test)]
mod decoder_limit_tests {
    #![allow(clippy::unwrap_used)]
    #[test]
    fn a_valid_png_with_excess_dimensions_is_rejected_before_decode_allocation() {
        let pixels = image::RgbaImage::new(1025, 1);
        let mut bytes = Vec::new();
        image::ImageEncoder::write_image(
            image::codecs::png::PngEncoder::new(&mut bytes),
            pixels.as_raw(),
            1025,
            1,
            image::ExtendedColorType::Rgba8,
        )
        .unwrap();
        assert!(bytes.len() < crate::MAX_BYTES);
        assert!(matches!(
            super::dimensions(&bytes),
            Err(super::ImageError::Codec(image::ImageError::Limits(_)))
        ));
    }
}
