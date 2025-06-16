ruff check app/ > output.txt
sleep 5
linter_val=$(tail -2 output.txt | head -1)
rm output.txt
s="$linter_val" && A="$(cut -d' ' -f2 <<<"$s")"

# the only value to be altered in this script
upper_limit=512

if [ "$A" -le "$upper_limit" ]; then
    echo "Ruff Lint check passed with "$A" pre-existing issues."

else
    echo "Ruff Lint check failed with $((A-upper_limit)) new and "$A" total issues."
    echo "Please trigger ruff check <directory_or_file> locally to find and squash the issues."
    exit 1

fi