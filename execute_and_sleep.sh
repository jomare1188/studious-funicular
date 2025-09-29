#!/bin/bash


# function sleep with progress
sleep_with_progress() {
  local total_seconds=$1
  local interval=600 #10minutes
  local elapsed=0

  while [ $elapsed -lt $total_seconds ]; do
    sleep $interval
    elapsed=$((elapsed + interval))
    if [ $elapsed -gt $total_seconds ]; then
      elapsed=$total_seconds
    fi
    remaining=$((total_seconds - elapsed))
    echo "Passed $((elapsed / 60)) minutes, $((remaining / 60)) minutes left until next command..."
  done
}

echo "Waiting"
sleep_with_progress 86400

uv run code/get_pdf_from_json.py --input scholar_results/json/citrus/dna/ --apikeys /home/j/Downloads/api.keys --email matheuspimenta@usp.br

# wait
echo "Waiting ..."
sleep_with_progress 86400

uv run code/get_pdf_from_json.py --input scholar_results/json/sugarcane/rna/ --apikeys /home/j/Downloads/api.keys --email matheuspimenta@usp.br

# wait
echo "Waiting ..."
sleep_with_progress 86400
uv run code/get_pdf_from_json.py --input scholar_results/json/sugarcane/dna/ --apikeys /home/j/Downloads/api.keys --email matheuspimenta@usp.br

