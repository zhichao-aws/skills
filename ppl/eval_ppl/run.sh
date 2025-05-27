python evaluate_ppl.py --ppl_file dataset/time_related_queries.json
python evaluate_ppl.py --ppl_file generated_ppls/time_related_queries.json

python compare_results.py \
    --ground_truth_file results/dataset-time_related_queries.json \
    --to_eval_file results/generated_ppls-time_related_queries.json \
    --target_file results/pairs/gold_vs_ppl.json