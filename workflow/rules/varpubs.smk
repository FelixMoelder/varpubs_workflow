

rule varpubs_deploy_db:
    input:
        bcf="results/calls/{group}.somatic_vus.variants.fdr-controlled.bcf",
    output:
        "results/varpubs/{group}.somatic_vus.duckdb",
    conda:
        "../envs/varpubs.yaml"
    resources:
        varpubs_api=1
    log:
        "logs/varpub/deploy/{group}.log",
    shell:
        "varpubs -v deploy-db --db_path {output} --vcf_paths {input.bcf} &> {log}"


rule varpubs_summarize_variants:
    input:
        bcf="results/calls/{group}.somatic_vus.variants.fdr-controlled.bcf",
        db_path="results/varpubs/{group}.somatic_vus.duckdb",
        cache=get_unmodified_varpubs_cache(),
    output:
        summaries="results/varpubs/{group}.somatic_vus.csv",
        cache="results/varpubs/caches/{group}.somatic_vus.duckdb",
    params:
        llm_url=lookup(dpath="varpubs/llm_url", within=config),
        model=lookup(dpath="varpubs/model", within=config),
        api_key=lookup(dpath="varpubs/api_key", within=config),
        cache=lambda wc, input: f"--cache {input.cache}" if input.cache else ""
    conda:
        "../envs/varpubs.yaml"
    log:
        "logs/varpub/summarize/{group}.log",
    threads: max(workflow.cores, 1)
    shell:
        "varpubs -v summarize-variants --db_path {input.db_path} --vcf_path {input.bcf} --llm_url {params.llm_url} --model {params.model} --api_key '{params.api_key}' --judges 'therapy related' {params.cache} --output {output.summaries} --output_cache {output.cache} &> {log}"


rule datavzrd_varpubs:
    input:
        csv="results/varpubs/{group}.somatic_vus.csv",
        config=workflow.source_path(
            "../resources/datavzrd/varpubs-template.datavzrd.yaml"
        ),
        summary_formatter=workflow.source_path(
            "../resources/datavzrd/summary_formatter.js"
        ),
    output:
        report(
            directory(
                "results/datavzrd-report/varpubs/{group}.somatic_vus"
            ),
            htmlindex="index.html",
            category="Summaries of variant publications",
            labels=lambda wc: {"sample": wc.group},
            subcategory="VUS",
        ),
    log:
        "logs/datavzrd_report/varpubs/{group}.log",
    wrapper:
        "v8.0.3/utils/datavzrd"


rule merge_caches:
    input:
        cache=get_unmodified_varpubs_cache(),
        event_caches=get_varpubs_sample_caches()
    output:
        update(
            lookup(dpath="varpubs/cache", within=config)
        )
    log:
        "logs/varpub/merge_cache.log",
    conda:
        "../envs/varpubs.yaml"
    shell:
        "varpubs update-cache --cache {input.event_caches} --output {output} &> {log}"
