# function to check and echo accordingly
check_and_print() {
    if [ "$1" -le "$2" ]; then
        echo "Bandit check passed with "$1" pre-existing "$3" issues."

    else
        echo "Bandit check failed with $(( $1 - $2 )) new and "$1" total "$3" issues."
        echo "Please trigger bandit -c bandit.yml --recursive . locally to find and quash the issues."
        exit 1

    fi
}

# triggering bandit over the parent directory recursively and routing the output to a temporary output file
# earlier command: bandit --exclude '**/*venv*/**' --recursive . > output_bandit.txt
bandit -c bandit.yml --recursive . > output_bandit.txt
sleep 5

# extracting the summary section of the bandit output
summary=$(tail -16 output_bandit.txt)

: '
Sample of the 16 lines extracted above:

1. Code scanned:
2.         Total lines of code: 18157
3.         Total lines skipped (#nosec): 55
4. 
5. Run metrics:
6.         Total issues (by severity):
7.                Undefined: 0
8.                Low: 1
9.                Medium: 0
10.               High: 0
11.        Total issues (by confidence):
12.               Undefined: 0
13.               Low: 0
14.               Medium: 1
15.               High: 0
16. Files skipped (0):
'

# purging the temporary output file
rm output_bandit.txt

# four approaches present, going ahead with 2 for brevity
# 1. count=$(echo "$issue" | cut -d':' -f2 | xargs)
# 2. count="${issue#*: }"
# 3. count=$(echo "$issue" | sed 's/^[^:]*: //' | xargs)
# 4. count=$(echo "$issue" | awk -F': ' '{print $2}')

# undefined
undefined_issue_count_upper_limit=0
undefined_issue=$(echo "$summary" | awk 'NR==7')
undefined_issue_count="${undefined_issue#*: }"
check_and_print "$undefined_issue_count" "$undefined_issue_count_upper_limit" "undefined"

# low
low_issue_count_upper_limit=1
low_issue=$(echo "$summary" | awk 'NR==8')
low_issue_count="${low_issue#*: }"
check_and_print "$low_issue_count" "$low_issue_count_upper_limit" "low"

# medium
medium_issue_count_upper_limit=0
medium_issue=$(echo "$summary" | awk 'NR==9')
medium_issue_count="${medium_issue#*: }"
check_and_print "$medium_issue_count" "$medium_issue_count_upper_limit" "medium"

# high
high_issue_count_upper_limit=0
high_issue=$(echo "$summary" | awk 'NR==10')
high_issue_count="${high_issue#*: }"
check_and_print "$high_issue_count" "$high_issue_count_upper_limit" "high"
