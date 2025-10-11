from flask import Flask, render_template
import pandas as pd

app = Flask(__name__)

@app.route('/')
def index():
    # Example DataFrame
    data = {
        "Name": ["Alice", "Bob", "Charlie"],
        "Age": [25, 30, 35],
        "City": ["New York", "Los Angeles", "Chicago"]
    }
    df = pd.DataFrame(data)

    # Convert DataFrame to HTML
    table_html = df.to_html(classes='table table-striped', index=False)

    return render_template('index.html', table=table_html)

if __name__ == '__main__':
    app.run(debug=True)
