"""Codex 请求内联图片压缩，限制发往模型上游的单张图片大小。"""

import base64
import io
import json
import logging

from PIL import Image, ImageOps


logger = logging.getLogger(__name__)

_MAX_IMAGE_BYTES = 250000
_SUPPORTED_DATA_URI_PREFIXES = (
    "data:image/png;base64,",
    "data:image/jpeg;base64,",
    "data:image/jpg;base64,",
)


def _rewrite_payload_value(value, route, stats):
    """递归复制 Codex JSON 值，并压缩超过上限的内联 PNG/JPEG 图片。"""
    if isinstance(value, dict):
        return {
            key: _rewrite_payload_value(item, route, stats)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_rewrite_payload_value(item, route, stats) for item in value]
    if not isinstance(value, str):
        return value

    matched_prefix = None
    for prefix in _SUPPORTED_DATA_URI_PREFIXES:
        if value.startswith(prefix):
            matched_prefix = prefix
            break
    if matched_prefix is None:
        return value

    try:
        original_bytes = base64.b64decode(value[len(matched_prefix):], validate=True)
        if len(original_bytes) <= _MAX_IMAGE_BYTES:
            return value

        with Image.open(io.BytesIO(original_bytes)) as source_image:
            source_image.load()
            icc_profile = source_image.info.get("icc_profile")
            image = ImageOps.exif_transpose(source_image)
            has_transparency = (
                "A" in image.getbands() or "transparency" in image.info
            )
            if has_transparency:
                rgba_image = image.convert("RGBA")
                black_background = Image.new(
                    "RGBA", rgba_image.size, (0, 0, 0, 255)
                )
                image = Image.alpha_composite(
                    black_background, rgba_image
                ).convert("RGB")
            else:
                image = image.convert("RGB")

            while True:
                for quality in range(95, 29, -5):
                    jpeg_buffer = io.BytesIO()
                    save_options = {
                        "format": "JPEG",
                        "quality": quality,
                        "optimize": True,
                        "progressive": True,
                    }
                    if icc_profile:
                        save_options["icc_profile"] = icc_profile
                    image.save(jpeg_buffer, **save_options)
                    jpeg_bytes = jpeg_buffer.getvalue()
                    if len(jpeg_bytes) <= _MAX_IMAGE_BYTES:
                        stats["compressed_count"] += 1
                        stats["original_bytes"] += len(original_bytes)
                        stats["jpeg_bytes"] += len(jpeg_bytes)
                        return "data:image/jpeg;base64," + base64.b64encode(
                            jpeg_bytes
                        ).decode("ascii")

                resized_width = max(1, int(image.width * 0.9))
                resized_height = max(1, int(image.height * 0.9))
                if (resized_width, resized_height) == image.size:
                    raise ValueError("JPEG image cannot be reduced below the byte limit")
                image = image.resize(
                    (resized_width, resized_height), Image.Resampling.LANCZOS
                )
    except Exception:
        logger.exception(
            "Codex inline image compression failed; preserving original image (route=%s)",
            route,
        )
        return value


def compress_codex_image_data_uris(payload, route="codex"):
    """压缩 Codex JSON 结构中超过 250000 字节的内联 PNG/JPEG 图片。"""
    stats = {
        "compressed_count": 0,  # 成功转换为 JPEG 的图片张数。
        "original_bytes": 0,  # 成功转换图片的原始二进制总字节数。
        "jpeg_bytes": 0,  # 成功转换后 JPEG 二进制总字节数。
    }
    updated_payload = _rewrite_payload_value(payload, route, stats)
    if stats["compressed_count"]:
        logger.info(
            "Codex inline image compression completed route=%s images=%d original_bytes=%d jpeg_bytes=%d",
            route,
            stats["compressed_count"],
            stats["original_bytes"],
            stats["jpeg_bytes"],
        )
    return updated_payload, stats


def compress_codex_image_json_body(body, route="codex"):
    """压缩 Codex JSON HTTP body 中的内联图片，并在无转换时原样返回 body。"""
    if not isinstance(body, (str, bytes)):
        return body, {
            "compressed_count": 0,  # 成功转换为 JPEG 的图片张数。
            "original_bytes": 0,  # 成功转换图片的原始二进制总字节数。
            "jpeg_bytes": 0,  # 成功转换后 JPEG 二进制总字节数。
        }

    try:
        payload = json.loads(body)
    except (TypeError, ValueError):
        logger.exception(
            "Codex JSON body parsing failed; preserving original request body (route=%s)",
            route,
        )
        return body, {
            "compressed_count": 0,  # 成功转换为 JPEG 的图片张数。
            "original_bytes": 0,  # 成功转换图片的原始二进制总字节数。
            "jpeg_bytes": 0,  # 成功转换后 JPEG 二进制总字节数。
        }

    updated_payload, stats = compress_codex_image_data_uris(payload, route=route)
    if not stats["compressed_count"]:
        return body, stats

    updated_body = json.dumps(updated_payload, ensure_ascii=False)
    if isinstance(body, bytes):
        return updated_body.encode("utf-8"), stats
    return updated_body, stats
