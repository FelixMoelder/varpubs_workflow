def get_unmodified_varpubs_cache():
    cache = lookup(dpath="varpubs/cache", within=config)
    if cache != "" and not cache.endswith(".duckdb"):
        raise ValueError("Varpubs cache must be a duckdb file.")
    if exists(cache):
        return before_update(cache)
    else:
        return []


def get_varpubs_sample_caches():
    pattern = "results/varpubs/caches/{group}.somatic_vus.duckdb"
    return expand(
            pattern,
            group=groups,
        )
