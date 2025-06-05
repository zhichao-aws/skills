set -e

# Define file and directory variables
BENCH_FILE="non_time_related_queries.json"
EVAL_DIR="eval_ppl"
DATASET_DIR="dataset"
GENERATED_DIR="generated_ppls"
RESULTS_DIR="results"
GOLD_DIR="${RESULTS_DIR}/gold"
BASELINE_DIR="${RESULTS_DIR}/baseline"
EVAL_RESULTS_DIR="${RESULTS_DIR}/eval_res"

# Change to evaluation directory
cd ${EVAL_DIR}

# Step 1: Generate PPL queries
python generate_ppl_query.py \
    --bench_file ${BENCH_FILE}

# Step 2: Run PPL on gold standard data
python run_ppl.py \
    --bench_file ${BENCH_FILE} \
    --ppl_root ${DATASET_DIR} \
    --output_root ${GOLD_DIR}

# Step 3: Run PPL on generated data
python run_ppl.py \
    --bench_file ${BENCH_FILE} \
    --ppl_root ${GENERATED_DIR} \
    --output_root ${BASELINE_DIR}

# Step 4: Compare the results
python compare_results.py \
    --label_root ${GOLD_DIR} \
    --results_root ${BASELINE_DIR} \
    --target_root ${EVAL_RESULTS_DIR} \
    --bench_file ${BENCH_FILE}
