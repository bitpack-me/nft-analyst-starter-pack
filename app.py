from flask import Flask, send_from_directory, current_app
from flask_executor import Executor
import os
from flask_shell2http import Shell2HTTP

# Flask application instance
app = Flask(__name__)

@app.route('/download/<path:filename>', methods=['GET'])
def download_file(filename):
  uploads = os.path.join(current_app.root_path)
  print(uploads, filename)
  return send_from_directory(uploads, filename, as_attachment=True)

executor = Executor(app)
shell2http = Shell2HTTP(app=app, executor=executor, base_url_prefix="/commands/")



def my_callback_fn(context, future):
  # optional user-defined callback function
  print(context, future.result())

shell2http.register_command(endpoint="export_data",
  command_name="python3 export_data.py",
  callback_fn=my_callback_fn,
  decorators=[]
)

shell2http.register_command(endpoint="echo",
  command_name="echo",
  callback_fn=my_callback_fn,
  decorators=[]
)
