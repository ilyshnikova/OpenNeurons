from .env import app, base

@app.route('/user')
@required_roles('admin', 'user')
def user_page(self):
    return "You've got permission to access this page."
