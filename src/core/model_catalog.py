"""Canonical Flow image & video model catalog (public models + legacy aliases)."""

from __future__ import annotations

from typing import Any, Dict, Optional

IMAGE_ASPECT_RATIOS: tuple[tuple[str, str], ...] = (
    ("landscape", "IMAGE_ASPECT_RATIO_LANDSCAPE"),
    ("portrait", "IMAGE_ASPECT_RATIO_PORTRAIT"),
    ("square", "IMAGE_ASPECT_RATIO_SQUARE"),
    ("four-three", "IMAGE_ASPECT_RATIO_LANDSCAPE_FOUR_THREE"),
    ("three-four", "IMAGE_ASPECT_RATIO_PORTRAIT_THREE_FOUR"),
)

IMAGE_UPSAMPLE_TIERS: tuple[tuple[str, Optional[str]], ...] = (
    ("", None),
    ("-2k", "UPSAMPLE_IMAGE_RESOLUTION_2K"),
    ("-4k", "UPSAMPLE_IMAGE_RESOLUTION_4K"),
)

VIDEO_LANDSCAPE = "VIDEO_ASPECT_RATIO_LANDSCAPE"
VIDEO_PORTRAIT = "VIDEO_ASPECT_RATIO_PORTRAIT"


def _make_t2v_config(
    model_key: str,
    aspect_ratio: str,
    *,
    use_v2_model_config: bool = False,
    allow_tier_upgrade: bool = True,
    upsample: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "type": "video",
        "video_type": "t2v",
        "model_key": model_key,
        "aspect_ratio": aspect_ratio,
        "supports_images": False,
    }
    if use_v2_model_config:
        cfg["use_v2_model_config"] = True
    if not allow_tier_upgrade:
        cfg["allow_tier_upgrade"] = False
    if upsample:
        cfg["upsample"] = upsample
    return cfg


def _make_i2v_config(
    model_key: str,
    aspect_ratio: str,
    *,
    min_images: int = 1,
    max_images: int = 2,
    use_v2_model_config: bool = False,
    allow_tier_upgrade: bool = True,
    upsample: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {
        "type": "video",
        "video_type": "i2v",
        "model_key": model_key,
        "aspect_ratio": aspect_ratio,
        "supports_images": True,
        "min_images": min_images,
        "max_images": max_images,
    }
    if use_v2_model_config:
        cfg["use_v2_model_config"] = True
    if not allow_tier_upgrade:
        cfg["allow_tier_upgrade"] = False
    if upsample:
        cfg["upsample"] = upsample
    return cfg


def _build_model_catalog() -> Dict[str, Dict[str, Any]]:
    """Build the full Flow model catalog (public entries + hidden compatibility aliases)."""
    catalog: Dict[str, Dict[str, Any]] = {}

    def register(model_id: str, config_entry: Dict[str, Any], display_name: str) -> None:
        entry = dict(config_entry)
        entry["listed"] = True
        entry["display_name"] = display_name
        catalog[model_id] = entry

    def register_hidden(model_id: str, config_entry: Dict[str, Any]) -> None:
        entry = dict(config_entry)
        entry["listed"] = False
        catalog[model_id] = entry

    def register_hidden_upsamples(model_id: str, base_cfg: Dict[str, Any]) -> None:
        for res_name, res_enum, up_key in (
            ("1080p", "VIDEO_RESOLUTION_1080P", "veo_3_1_upsampler_1080p"),
            ("4k", "VIDEO_RESOLUTION_4K", "veo_3_1_upsampler_4k"),
        ):
            register_hidden(
                f"{model_id}-{res_name}",
                {
                    **base_cfg,
                    "upsample": {"resolution": res_enum, "model_key": up_key},
                },
            )

    def t2v(model_key: str, aspect_ratio: str) -> Dict[str, Any]:
        return _make_t2v_config(
            model_key, aspect_ratio, use_v2_model_config=True, allow_tier_upgrade=False
        )

    def i2v(model_key: str, aspect_ratio: str, max_images: int = 2) -> Dict[str, Any]:
        return _make_i2v_config(
            model_key,
            aspect_ratio,
            min_images=1,
            max_images=max_images,
            use_v2_model_config=True,
            allow_tier_upgrade=False,
        )

    def r2v(model_key: str, aspect_ratio: str) -> Dict[str, Any]:
        return {
            "type": "video",
            "video_type": "r2v",
            "model_key": model_key,
            "aspect_ratio": aspect_ratio,
            "supports_images": True,
            "min_images": 1,
            "max_images": 3,
            "use_v2_model_config": True,
            "allow_tier_upgrade": False,
        }

    def extend(model_key: str, aspect_ratio: str) -> Dict[str, Any]:
        return {
            "type": "video",
            "video_type": "extend",
            "model_key": model_key,
            "aspect_ratio": aspect_ratio,
            "supports_images": False,
            "use_v2_model_config": True,
            "allow_tier_upgrade": False,
        }

    def omni_cfg(
        family_prefix: str,
        seconds: int,
        aspect_ratio: str,
        *,
        display_label: str,
        resolution: Optional[str] = None,
        upsample: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        res_suffix = "_360p" if resolution == "360p" else ""
        cfg: Dict[str, Any] = {
            "type": "video",
            "video_type": "omni",
            "model_key": f"{family_prefix}_t2v_{seconds}s{res_suffix}",
            "reference_model_key": f"{family_prefix}_r2v_{seconds}s{res_suffix}",
            "reference_duration": seconds,
            "reference_model_display_name": display_label,
            "aspect_ratio": aspect_ratio,
            "supports_images": True,
            "min_images": 0,
            "max_images": 3,
            "use_v2_model_config": True,
            "allow_tier_upgrade": False,
        }
        if resolution:
            cfg["video_resolution"] = resolution
        if upsample:
            cfg["upsample"] = upsample
        return cfg

    # 1. Canonical public image models (default 16:9 + 2K; controlled via aspectRatio & imageSize)
    #    All 45 legacy ratio/resolution suffixed variants stay registered as hidden aliases.
    image_default_params: Dict[str, Any] = {
        "default_aspect_ratio": "16:9",
        "supported_aspect_ratios": ["16:9", "9:16", "1:1", "4:3", "3:4"],
        "default_image_size": "2K",
        "supported_image_sizes": ["1K", "2K", "4K"],
    }
    for prefix, upstream_name, display_name in (
        ("gemini-3.1-flash-image", "BELUGA", "Nano Banana 2.1"),
        ("gemini-3.1-flash-lite-image", "HARBOR_SEAL", "Nano Banana 2 Lite"),
        ("gemini-3.0-pro-image", "GEM_PIX_2", "Nano Banana Pro"),
    ):
        register(
            prefix,
            {
                "type": "image",
                "model_name": upstream_name,
                "aspect_ratio": "IMAGE_ASPECT_RATIO_LANDSCAPE",
                "upsample": "UPSAMPLE_IMAGE_RESOLUTION_2K",
                **image_default_params,
            },
            f"{display_name} · 16:9 / 9:16 / 1:1 / 4:3 / 3:4 · 1K / 2K / 4K",
        )
        for suffix, upsample_enum in IMAGE_UPSAMPLE_TIERS:
            for ratio_slug, ratio_enum in IMAGE_ASPECT_RATIOS:
                entry: Dict[str, Any] = {
                    "type": "image",
                    "model_name": upstream_name,
                    "aspect_ratio": ratio_enum,
                    "display_name": display_name,
                }
                if upsample_enum:
                    entry["upsample"] = upsample_enum
                register_hidden(f"{prefix}-{ratio_slug}{suffix}", entry)
    # 2. Hidden legacy Veo 3.1 Lite & Omni shorthand aliases
    for orientation, ratio in (("portrait", VIDEO_PORTRAIT), ("landscape", VIDEO_LANDSCAPE)):
        register_hidden(f"veo_3_1_t2v_lite_{orientation}", t2v("veo_3_1_t2v_lite", ratio))
        register_hidden(
            f"veo_3_1_i2v_lite_{orientation}",
            i2v("veo_3_1_i2v_lite", ratio, max_images=1),
        )
        register_hidden(
            f"veo_3_1_interpolation_lite_{orientation}",
            _make_i2v_config(
                "veo_3_1_interpolation_lite",
                ratio,
                min_images=2,
                max_images=2,
                use_v2_model_config=True,
                allow_tier_upgrade=False,
            ),
        )
        for seconds in (4, 6):
            sfx = f"{seconds}s"
            for alias_fmt, cfg in (
                (f"veo_3_1_t2v_lite_{sfx}_{orientation}", t2v(f"veo_3_1_t2v_lite_{sfx}", ratio)),
                (
                    f"veo_3_1_i2v_lite_{sfx}_{orientation}",
                    i2v(f"veo_3_1_i2v_s_lite_{sfx}", ratio, max_images=1),
                ),
                (
                    f"veo_3_1_interpolation_lite_{sfx}_{orientation}",
                    _make_i2v_config(
                        f"veo_3_1_i2v_s_lite_{sfx}_fl",
                        ratio,
                        min_images=2,
                        max_images=2,
                        use_v2_model_config=True,
                        allow_tier_upgrade=False,
                    ),
                ),
            ):
                register_hidden(alias_fmt, cfg)
            register_hidden(
                f"veo_3_1_t2v_lite_{orientation}_{sfx}",
                catalog[f"veo_3_1_t2v_lite_{sfx}_{orientation}"],
            )
            register_hidden(
                f"veo_3_1_i2v_lite_{orientation}_{sfx}",
                catalog[f"veo_3_1_i2v_lite_{sfx}_{orientation}"],
            )
            register_hidden(
                f"veo_3_1_interpolation_lite_{orientation}_{sfx}",
                catalog[f"veo_3_1_interpolation_lite_{sfx}_{orientation}"],
            )

        for base_prefix in (
            "veo_3_1_t2v_lite",
            "veo_3_1_i2v_lite",
            "veo_3_1_interpolation_lite",
        ):
            target = catalog[f"{base_prefix}_{orientation}"]
            register_hidden(f"{base_prefix}_8s_{orientation}", target)
            register_hidden(f"{base_prefix}_{orientation}_8s", target)

    register_hidden(
        "omni", omni_cfg("macaron_cookie", 8, VIDEO_LANDSCAPE, display_label="Omni 1.2 Flash")
    )
    register_hidden(
        "omni_portrait",
        omni_cfg("macaron_cookie", 8, VIDEO_PORTRAIT, display_label="Omni 1.2 Flash"),
    )

    # 3. Legacy Veo 3.1 Lite T2V suffixed variants (hidden; canonical `veo-3.1-lite` listed below)
    for seconds, key in (
        (4, "veo_3_1_t2v_lite_4s"),
        (6, "veo_3_1_t2v_lite_6s"),
        (8, "veo_3_1_t2v_lite"),
    ):
        for orientation, ratio in (("landscape", VIDEO_LANDSCAPE), ("portrait", VIDEO_PORTRAIT)):
            model_id = f"veo-3.1-lite-{seconds}s-{orientation}"
            cfg = t2v(key, ratio)
            register_hidden(model_id, {**cfg, "display_name": f"Veo 3.1 - Lite · {seconds}s · {orientation}"})
            register_hidden_upsamples(model_id, cfg)

    # 4. Legacy Omni 1.2 Flash (macaron_cookie) 3s-15s suffixed variants (hidden; canonical models listed below)
    for seconds in range(3, 16):
        for orientation, ratio in (("landscape", VIDEO_LANDSCAPE), ("portrait", VIDEO_PORTRAIT)):
            cfg_720 = omni_cfg("macaron_cookie", seconds, ratio, display_label="Omni 1.2 Flash")
            cfg_360 = omni_cfg(
                "macaron_cookie", seconds, ratio, display_label="Omni 1.2 Flash", resolution="360p"
            )
            model_id = f"omni-1.2-flash-{seconds}s-{orientation}"
            model_id_360 = f"{model_id}-360p"
            register_hidden(model_id, {**cfg_720, "display_name": f"Omni 1.2 Flash · {seconds}s · {orientation}"})
            register_hidden(
                model_id_360,
                {**cfg_360, "display_name": f"Omni 1.2 Flash · {seconds}s · {orientation} · 360p"},
            )
            register_hidden_upsamples(model_id, cfg_720)

            if seconds <= 10:
                i2v_720 = {
                    **i2v(f"macaron_cookie_i2v_fl_{seconds}s", ratio, max_images=2),
                    "start_frame_model_key": f"macaron_cookie_i2v_{seconds}s",
                }
                i2v_360 = {
                    **i2v(f"macaron_cookie_i2v_fl_{seconds}s_360p", ratio, max_images=2),
                    "start_frame_model_key": f"macaron_cookie_i2v_{seconds}s_360p",
                    "video_resolution": "360p",
                }
            else:
                i2v_720 = i2v(f"macaron_cookie_i2v_{seconds}s", ratio, max_images=1)
                i2v_360 = {
                    **i2v(f"macaron_cookie_i2v_{seconds}s_360p", ratio, max_images=1),
                    "video_resolution": "360p",
                }
            i2v_id = f"omni-1.2-flash-i2v-{seconds}s-{orientation}"
            register_hidden(i2v_id, {**i2v_720, "display_name": f"Omni 1.2 Flash I2V · {seconds}s · {orientation}"})
            register_hidden(f"{i2v_id}-360p", i2v_360)
            register_hidden_upsamples(i2v_id, i2v_720)

            if seconds <= 10:
                ext_720 = extend(f"macaron_cookie_extend_{seconds}s", ratio)
                ext_360 = {
                    **extend(f"macaron_cookie_extend_{seconds}s_360p", ratio),
                    "video_resolution": "360p",
                }
                ext_id = f"omni-1.2-flash-extend-{seconds}s-{orientation}"
                register_hidden(
                    ext_id,
                    {**ext_720, "display_name": f"Omni 1.2 Flash Extend · {seconds}s · {orientation}"},
                )
                register_hidden(f"{ext_id}-360p", ext_360)
                register_hidden_upsamples(ext_id, ext_720)
        # Hidden raw upstream usage key aliases
        register_hidden(
            f"macaron_cookie_t2v_{seconds}s",
            omni_cfg("macaron_cookie", seconds, VIDEO_LANDSCAPE, display_label="Omni 1.2 Flash"),
        )
        register_hidden(
            f"macaron_cookie_t2v_{seconds}s_360p",
            omni_cfg(
                "macaron_cookie",
                seconds,
                VIDEO_LANDSCAPE,
                display_label="Omni 1.2 Flash",
                resolution="360p",
            ),
        )
        register_hidden(
            f"macaron_cookie_r2v_{seconds}s",
            omni_cfg("macaron_cookie", seconds, VIDEO_LANDSCAPE, display_label="Omni 1.2 Flash"),
        )
        register_hidden(
            f"macaron_cookie_r2v_{seconds}s_360p",
            omni_cfg(
                "macaron_cookie",
                seconds,
                VIDEO_LANDSCAPE,
                display_label="Omni 1.2 Flash",
                resolution="360p",
            ),
        )
        register_hidden(
            f"macaron_cookie_i2v_{seconds}s",
            i2v(f"macaron_cookie_i2v_{seconds}s", VIDEO_LANDSCAPE, max_images=1),
        )
        register_hidden(
            f"macaron_cookie_i2v_{seconds}s_360p",
            {
                **i2v(f"macaron_cookie_i2v_{seconds}s_360p", VIDEO_LANDSCAPE, max_images=1),
                "video_resolution": "360p",
            },
        )
        if seconds <= 10:
            register_hidden(
                f"macaron_cookie_i2v_fl_{seconds}s",
                {
                    **i2v(f"macaron_cookie_i2v_fl_{seconds}s", VIDEO_LANDSCAPE, max_images=2),
                    "start_frame_model_key": f"macaron_cookie_i2v_{seconds}s",
                },
            )
            register_hidden(
                f"macaron_cookie_i2v_fl_{seconds}s_360p",
                {
                    **i2v(
                        f"macaron_cookie_i2v_fl_{seconds}s_360p", VIDEO_LANDSCAPE, max_images=2
                    ),
                    "start_frame_model_key": f"macaron_cookie_i2v_{seconds}s_360p",
                    "video_resolution": "360p",
                },
            )
            register_hidden(
                f"macaron_cookie_extend_{seconds}s",
                extend(f"macaron_cookie_extend_{seconds}s", VIDEO_LANDSCAPE),
            )
            register_hidden(
                f"macaron_cookie_extend_{seconds}s_360p",
                {
                    **extend(f"macaron_cookie_extend_{seconds}s_360p", VIDEO_LANDSCAPE),
                    "video_resolution": "360p",
                },
            )

    for orientation, ratio in (("landscape", VIDEO_LANDSCAPE), ("portrait", VIDEO_PORTRAIT)):
        base_8 = omni_cfg("macaron_cookie", 8, ratio, display_label="Omni 1.2 Flash")
        register_hidden(f"omni-1.2-flash-{orientation}", base_8)
        register_hidden(
            f"omni-1.2-flash-{orientation}-360p",
            omni_cfg("macaron_cookie", 8, ratio, display_label="Omni 1.2 Flash", resolution="360p"),
        )
        register_hidden_upsamples(f"omni-1.2-flash-{orientation}", base_8)

        i2v_8 = {
            **i2v("macaron_cookie_i2v_fl_8s", ratio, max_images=2),
            "start_frame_model_key": "macaron_cookie_i2v_8s",
        }
        register_hidden(f"omni-1.2-flash-i2v-{orientation}", i2v_8)
        register_hidden(
            f"omni-1.2-flash-i2v-{orientation}-360p",
            {
                **i2v("macaron_cookie_i2v_fl_8s_360p", ratio, max_images=2),
                "start_frame_model_key": "macaron_cookie_i2v_8s_360p",
                "video_resolution": "360p",
            },
        )
        register_hidden_upsamples(f"omni-1.2-flash-i2v-{orientation}", i2v_8)

        ext_8 = extend("macaron_cookie_extend_8s", ratio)
        register_hidden(f"omni-1.2-flash-extend-{orientation}", ext_8)
        register_hidden(
            f"omni-1.2-flash-extend-{orientation}-360p",
            {**extend("macaron_cookie_extend_8s_360p", ratio), "video_resolution": "360p"},
        )
        register_hidden_upsamples(f"omni-1.2-flash-extend-{orientation}", ext_8)

    # 5. Legacy Veo 3.1 Lite I2V, R2V, Extend suffixed variants (hidden)
    for seconds, start_key, fl_key in (
        (4, "veo_3_1_i2v_s_lite_4s", "veo_3_1_i2v_s_lite_4s_fl"),
        (6, "veo_3_1_i2v_s_lite_6s", "veo_3_1_i2v_s_lite_6s_fl"),
        (8, "veo_3_1_i2v_lite", "veo_3_1_interpolation_lite"),
    ):
        for orientation, ratio in (("landscape", VIDEO_LANDSCAPE), ("portrait", VIDEO_PORTRAIT)):
            i2v_id = f"veo-3.1-lite-i2v-{seconds}s-{orientation}"
            i2v_cfg = {**i2v(fl_key, ratio), "start_frame_model_key": start_key}
            register_hidden(i2v_id, {**i2v_cfg, "display_name": f"Veo 3.1 - Lite I2V · {seconds}s · {orientation}"})
            register_hidden_upsamples(i2v_id, i2v_cfg)

    for orientation, ratio in (("landscape", VIDEO_LANDSCAPE), ("portrait", VIDEO_PORTRAIT)):
        r2v_id = f"veo-3.1-lite-r2v-8s-{orientation}"
        r2v_cfg = r2v("veo_3_1_r2v_lite", ratio)
        register_hidden(r2v_id, {**r2v_cfg, "display_name": f"Veo 3.1 - Lite R2V · 8s · {orientation}"})
        register_hidden_upsamples(r2v_id, r2v_cfg)
        ext_id = f"veo-3.1-lite-extend-8s-{orientation}"
        ext_cfg = extend("veo_3_1_extension_lite", ratio)
        register_hidden(ext_id, {**ext_cfg, "display_name": f"Veo 3.1 - Lite Extend · {orientation}"})
        register_hidden_upsamples(ext_id, ext_cfg)

    # 6. Canonical public video models (7 models; controlled via aspectRatio, durationSeconds, resolution)
    veo_common_params: Dict[str, Any] = {
        "default_aspect_ratio": "16:9",
        "supported_aspect_ratios": ["16:9", "9:16"],
        "default_duration": 8,
        "default_resolution": "720p",
        "supported_resolutions": ["720p", "1080p", "4K"],
    }
    omni_common_params: Dict[str, Any] = {
        "default_aspect_ratio": "16:9",
        "supported_aspect_ratios": ["16:9", "9:16"],
        "default_duration": 8,
        "default_resolution": "720p",
        "supported_resolutions": ["360p", "720p", "1080p", "4K"],
    }

    lite_default_cfg = {
        **t2v("veo_3_1_t2v_lite", VIDEO_LANDSCAPE),
        **veo_common_params,
        "supported_durations": [4, 6, 8],
    }
    register("veo-3.1-lite", lite_default_cfg, "Veo 3.1 Lite · 16:9 / 9:16 · 4s / 6s / 8s · 720p / 1080p / 4K")
    register_hidden_upsamples("veo-3.1-lite", t2v("veo_3_1_t2v_lite", VIDEO_LANDSCAPE))

    lite_i2v_default_cfg = {
        **i2v("veo_3_1_interpolation_lite", VIDEO_LANDSCAPE),
        "start_frame_model_key": "veo_3_1_i2v_lite",
        **veo_common_params,
        "supported_durations": [4, 6, 8],
    }
    register("veo-3.1-lite-i2v", lite_i2v_default_cfg, "Veo 3.1 Lite I2V · 16:9 / 9:16 · 4s / 6s / 8s · 720p / 1080p / 4K")
    register_hidden_upsamples("veo-3.1-lite-i2v", {**i2v("veo_3_1_interpolation_lite", VIDEO_LANDSCAPE), "start_frame_model_key": "veo_3_1_i2v_lite"})

    lite_r2v_default_cfg = {
        **r2v("veo_3_1_r2v_lite", VIDEO_LANDSCAPE),
        **veo_common_params,
        "supported_durations": [8],
    }
    register("veo-3.1-lite-r2v", lite_r2v_default_cfg, "Veo 3.1 Lite R2V · 16:9 / 9:16 · 8s · 720p / 1080p / 4K")
    register_hidden_upsamples("veo-3.1-lite-r2v", r2v("veo_3_1_r2v_lite", VIDEO_LANDSCAPE))

    lite_ext_default_cfg = {
        **extend("veo_3_1_extension_lite", VIDEO_LANDSCAPE),
        **veo_common_params,
        "supported_durations": [8],
    }
    register("veo-3.1-lite-extend", lite_ext_default_cfg, "Veo 3.1 Lite Extend · 16:9 / 9:16 · 8s · 720p / 1080p / 4K")
    register_hidden_upsamples("veo-3.1-lite-extend", extend("veo_3_1_extension_lite", VIDEO_LANDSCAPE))

    omni_12_default_cfg = {
        **omni_cfg("macaron_cookie", 8, VIDEO_LANDSCAPE, display_label="Omni 1.2 Flash"),
        **omni_common_params,
        "supported_durations": list(range(3, 16)),
    }
    register("omni-1.2-flash", omni_12_default_cfg, "Omni 1.2 Flash · 16:9 / 9:16 · 3s-15s · 360p / 720p / 1080p / 4K")
    register_hidden_upsamples(
        "omni-1.2-flash",
        omni_cfg("macaron_cookie", 8, VIDEO_LANDSCAPE, display_label="Omni 1.2 Flash"),
    )
    register_hidden(
        "omni-1.2-flash-360p",
        {
            **omni_cfg("macaron_cookie", 8, VIDEO_LANDSCAPE, display_label="Omni 1.2 Flash", resolution="360p"),
            "display_name": "Omni 1.2 Flash · 8s · landscape · 360p",
        },
    )

    omni_i2v_default_cfg = {
        **i2v("macaron_cookie_i2v_fl_8s", VIDEO_LANDSCAPE, max_images=2),
        "start_frame_model_key": "macaron_cookie_i2v_8s",
        **omni_common_params,
        "supported_durations": list(range(3, 16)),
    }
    register("omni-1.2-flash-i2v", omni_i2v_default_cfg, "Omni 1.2 Flash I2V · 16:9 / 9:16 · 3s-15s · 360p / 720p / 1080p / 4K")
    register_hidden_upsamples(
        "omni-1.2-flash-i2v",
        {**i2v("macaron_cookie_i2v_fl_8s", VIDEO_LANDSCAPE, max_images=2), "start_frame_model_key": "macaron_cookie_i2v_8s"},
    )

    omni_ext_default_cfg = {
        **extend("macaron_cookie_extend_8s", VIDEO_LANDSCAPE),
        **omni_common_params,
        "supported_durations": list(range(3, 11)),
    }
    register("omni-1.2-flash-extend", omni_ext_default_cfg, "Omni 1.2 Flash Extend · 16:9 / 9:16 · 3s-10s · 360p / 720p / 1080p / 4K")
    register_hidden_upsamples("omni-1.2-flash-extend", extend("macaron_cookie_extend_8s", VIDEO_LANDSCAPE))
    return catalog


MODEL_CONFIG: Dict[str, Dict[str, Any]] = _build_model_catalog()


def _known_video_model_keys() -> set[str]:
    keys = {
        cfg["model_key"]
        for cfg in MODEL_CONFIG.values()
        if cfg.get("type") == "video" and cfg.get("model_key")
    }
    keys.update(
        cfg["start_frame_model_key"]
        for cfg in MODEL_CONFIG.values()
        if cfg.get("type") == "video" and cfg.get("start_frame_model_key")
    )
    return keys


def _resolve_tier_two_model_key(model_key: str) -> str:
    """Only upgrade to an ultra key when that exact upstream key is known valid."""
    if "ultra" in model_key:
        return model_key
    if "_fl" in model_key:
        candidate = model_key.replace("_fl", "_ultra_fl")
    else:
        candidate = model_key + "_ultra"
    return candidate if candidate in _known_video_model_keys() else model_key
