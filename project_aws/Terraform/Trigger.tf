# Schedule
resource "aws_cloudwatch_event_rule" "scheduled_lambda_schedule" {
  schedule_expression ="rate(59 minutes)"
  name = "scheduled_lambda_schedule"
  description = "Schedule to trigger to_firehose function."
  is_enabled = true
}

resource "aws_cloudwatch_event_target" "scheduled_lambda_event_target" {
    rule = aws_cloudwatch_event_rule.scheduled_lambda_schedule.name
    target_id = "InvokeLambda"
    arn = aws_lambda_function.data_to_firehose.arn
}

resource "aws_lambda_permission" "scheduled_lambda_cloudwatch_permission" {
    statement_id = "AllowExecutionFromCloudWatch"
    action = "lambda:InvokeFunction"
    function_name = aws_lambda_function.data_to_firehose.arn
    principal = "events.amazonaws.com"
    source_arn = aws_cloudwatch_event_rule.scheduled_lambda_schedule.arn
}

