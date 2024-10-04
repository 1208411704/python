import re

from flask import Flask,request,render_template,session,redirect
from utils import query
app=Flask(__name__)
app.session_key =  'This is session_key you know ?'


@app.route('/login' ,methods=['GET','POST'])
def login():
    if request.method =='GET':
        return render_template('login.html')
    elif request.method =='POST':
        request.form=dict(request.form)
        def filter_fn(item):
         return request.form['email'] in item and request.form['password'] in item

        users=query.querys('select * from user',[],'select')
        filter_lsit=list(filter(filter_fn,users))


        if len(filter_user):
            session['email'] = request.form['email']
            return  redirect('/home')
        else:
            return render_template('error.html',message=' 邮箱或密码错误')
@app.route('/loginOut' )
def loginout():
    session.clear()
    return  redirect('/login')


@app.route('/register' ,methods=['GET','POST'])
def register():
    if request.method =='GET':
        return render_template('register.html')
    elif request.method =='POST':
        request.form =dict(request.form)
        if request.form['password'] != request.form['passwordChecked']:
            return render_template('error.html',message='两次密码不符合')
        def filter_fn(item):
            return request.form['email'] in item;
        users=query.querys('select *from user',[],'select')
        filter_list= list(filter(filter_fn,users))
        if len(filter_list):
            return render_template('error.html',message='用户已注册')
        else:
            query.querys('insert into user(email,password) values{%s,%s}',[request.form['email']],request.form['password'])
            return  redirect('/login')

@app.route('/home',methods=['GET','POST'])
def home():
    uname =session.get('email')
    return render_template(
        'index.html',
        email=email,

    )
    return render_template('home.html')
@app.before_request
def before_request():
    pathlib =re.compile(r'/static')
    if re.search(pat,request.path):
        return
    if request.path == "/login":
        return
    if request.path =='/registry':
        return
    uname =session.get('eamil')
    if uname:
        return  None
    return  redirect('/login')



@app.route('/')
def allRequest():
    return redirect('/login')
if __name__ =='__main__':
    app.run(debug= True)
