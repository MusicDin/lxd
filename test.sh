test=token123123
headers=("-H" "'Authorization: Basic ${test}'" "-H" "'Cookie: Dell: ${test};'")
echo "curl" ${headers[@]}
