from controllers import Controller

if __name__ == '__main__':
    # Instantiate the controller which sets up all services
    # and runs the Flask application.
    app_controller = Controller()
    app_controller.run()
