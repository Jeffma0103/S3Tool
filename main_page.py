import os
from pathlib import Path
from flask import Flask, render_template, request, send_from_directory
from s3tool import S3Helper
import requests

app = Flask(__name__)

def make_bold(fuction):
    def wrapper_fction():
        return "<b>" + fuction() + "</b>"
    return wrapper_fction

@app.route("/config")
def title_config():
    return render_template("index.html")

@app.route("/s3page", methods=["GET","POST"])
def title_s3page():
    if request.method == "POST" and request.form.get('session') != "" :
        global access_key
        global secret_key
        global session_token
        access_key = request.form.get('access')
        secret_key = request.form.get('secret')
        session_token = request.form.get('session')
        return render_template("s3page.html")
    else:
        return render_template("index_error.html")

@app.route("/s3page/bucket", methods=["GET", "POST"])
def bucket_list():
    result = S3Helper(access_key, secret_key, session_token)
    data = result.get_bucket_list()
    # return f"Bucket : {result.get_bucket_list()}"
    return render_template("bucket.html", data=data)

@app.route("/s3page/metadata", methods=["GET", "POST"])
def download_metadata():
    bucket_name, destination_folder = "videopass-staging-source-upload", "metadata/"
    file_name = "metadata.zip"
    result = S3Helper(access_key, secret_key, session_token)
    result.download_metadata(bucket_name, destination_folder)
    return send_from_directory("data", file_name, as_attachment=True)


@app.route("/s3page/mezzanine", methods=["GET", "POST"])
def download_mezzanine():
    bucket_name, destination_folder = "videopass-staging-source-upload", "1080p/"
    file_name = "mezzanine.mp4"
    result = S3Helper(access_key, secret_key, session_token)
    result.download_mezzanine(bucket_name, destination_folder)
    return send_from_directory("data", file_name, as_attachment=True)


if __name__ == "__main__":
    app.run(debug=True)