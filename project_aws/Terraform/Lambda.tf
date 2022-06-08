data "archive_file" "data_to_firehose" {
    output_path = "${path.module}/../Source/data_to_firehose.zip"
    source_file = "${path.module}/../Source/data_to_firehose.py"
    #excludes = ["__pycache__", "*.pyc"]
    type = "zip"
}

resource "aws_lambda_function" "data_to_firehose" {
    function_name = "data_to_firehose"
    filename = data.archive_file.data_to_firehose.output_path
    handler = "data_to_firehose.main"
    role = aws_iam_role.lambda_role.arn
    runtime = "python3.8"
    timeout = 180
}