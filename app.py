from flask import Flask
from flask import Flask, render_template, request
from olcademy import ask

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index_whoosh.html')

@app.route('/', methods=['POST'])
def result():
    
    q = request.args.get('search')
    
    #q = str(request.form.values())
            
    res = ask(str(q))
    
    return (render_template('index_whoosh.html', search_res=res))

if __name__=="__main__":
    app.run(debug=False)