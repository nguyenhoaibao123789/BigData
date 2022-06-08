resource "aws_s3_bucket" "csv" {
    bucket = "csv"
}

resource "aws_s3_bucket" "fact" {
    bucket = "fact-weather-hourly"
}

resource "aws_s3_bucket" "firehosebucket" {
  bucket = "firehosebucket"
}