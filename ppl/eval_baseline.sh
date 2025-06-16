set -e

# Define file and directory variables
BENCH_FILE="time_related_queries.json"
PPL_GENERATE_DIR="generated_ppls"
PPL_RESULTS_DIR="results/baseline"
TIME_DIR="None"

# Change to evaluation directory
cd eval_ppl

# Step 1: Generate PPL queries
# python generate_ppl_query.py --bench_file ${BENCH_FILE}

# Step 2: Run PPL on gold standard data
python run_ppl.py \
    --bench_file ${BENCH_FILE} \
    --ppl_root dataset \
    --output_root results/gold

# Step 3: Run PPL on generated data
python run_ppl.py \
    --bench_file ${BENCH_FILE} \
    --ppl_root ${PPL_GENERATE_DIR} \
    --output_root ${PPL_RESULTS_DIR} \
    --time_root ${TIME_DIR}

# Step 4: Compare the results
python compare_results.py \
    --label_root results/gold \
    --results_root ${PPL_RESULTS_DIR} \
    --target_root results/eval_res \
    --bench_file ${BENCH_FILE}
