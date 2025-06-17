set -e

DO_EXTRACTION=false
DO_GENETATE_PPL=false
DO_RUN_PPL_GOLD=false
DO_RUN_PPL=false
DO_RUN_PPL_COMBINED=false
DO_COMPARE_RESULTS=false

if [ "$DO_EXTRACTION" = true ]; then
    cd time_bench
    python extract_and_compare.py  --bench_file t2ppl_eval_wtime.json --prompt_file prompts/v7.txt
    python extract_and_compare.py  --bench_file t2ppl_eval_wotime.json --prompt_file prompts/v7.txt
    python extract_and_compare.py  --bench_file abs_time_queries.json --prompt_file prompts/v7.txt
    python extract_and_compare.py  --bench_file multi_time_queries.json --prompt_file prompts/v7.txt
    python extract_and_compare.py  --bench_file t2viz_eval.json --prompt_file prompts/v7.txt
    cd ..
fi

if [ "$DO_GENETATE_PPL" = true ]; then
    cd eval_ppl
    python generate_ppl_query.py --input_root dataset --bench_file t2ppl_eval_wtime.json
    python generate_ppl_query.py --input_root dataset --bench_file t2ppl_eval_wotime.json
    python generate_ppl_query.py --input_root dataset --bench_file abs_time_queries.json
    python generate_ppl_query.py --input_root dataset --bench_file multi_time_queries.json
    cd ..
fi

if [ "$DO_RUN_PPL_GOLD" = true ]; then
    cd eval_ppl
    python run_ppl.py --ppl_root dataset --bench_file t2ppl_eval_wtime.json
    python run_ppl.py --ppl_root dataset --bench_file t2ppl_eval_wotime.json
    python run_ppl.py --ppl_root dataset --bench_file abs_time_queries.json
    python run_ppl.py --ppl_root dataset --bench_file multi_time_queries.json
    cd ..
fi

if [ "$DO_RUN_PPL" = true ]; then
    cd eval_ppl
    python run_ppl.py --ppl_root generated_ppls --bench_file t2ppl_eval_wtime.json --output_root results_baseline
    python run_ppl.py --ppl_root generated_ppls --bench_file t2ppl_eval_wotime.json --output_root results_baseline
    python run_ppl.py --ppl_root generated_ppls --bench_file abs_time_queries.json --output_root results_baseline
    python run_ppl.py --ppl_root generated_ppls --bench_file multi_time_queries.json --output_root results_baseline
    cd ..
fi

if [ "$DO_RUN_PPL_COMBINED" = true ]; then
    cd eval_ppl
    python run_ppl.py --ppl_root generated_ppls --time_root dataset_parsed --bench_file t2ppl_eval_wtime.json --output_root results_time_parsed
    python run_ppl.py --ppl_root generated_ppls --time_root dataset_parsed --bench_file t2ppl_eval_wotime.json --output_root results_time_parsed
    python run_ppl.py --ppl_root generated_ppls --time_root dataset_parsed --bench_file abs_time_queries.json --output_root results_time_parsed
    python run_ppl.py --ppl_root generated_ppls --time_root dataset_parsed --bench_file multi_time_queries.json --output_root results_time_parsed
    cd ..
fi

if [ "$DO_COMPARE_RESULTS" = true ]; then
    cd eval_ppl
    python compare_results.py --label_root results --results_root results_baseline --bench_file t2ppl_eval_wtime.json
    python compare_results.py --label_root results --results_root results_time_parsed --bench_file t2ppl_eval_wtime.json
    python compare_results.py --label_root results --results_root results_baseline --bench_file t2ppl_eval_wotime.json
    python compare_results.py --label_root results --results_root results_time_parsed --bench_file t2ppl_eval_wotime.json
    python compare_results.py --label_root results --results_root results_baseline --bench_file abs_time_queries.json
    python compare_results.py --label_root results --results_root results_time_parsed --bench_file abs_time_queries.json
    python compare_results.py --label_root results --results_root results_baseline --bench_file multi_time_queries.json
    python compare_results.py --label_root results --results_root results_time_parsed --bench_file multi_time_queries.json
    cd ..
fi