import os
import sys
# DON'T CHANGE THIS !!!
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from flask import Flask, send_from_directory, render_template_string, request, jsonify
from flask_cors import CORS
import logging
from datetime import datetime

from src.database import db
from src.routes.wheels import wheels_bp
from src.routes.users import users_bp
from src.routes.data_integration import data_integration_bp
from src.services.analytics_service import analytics_service

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder=os.path.join(os.path.dirname(__file__), 'static'))
app.config['SECRET_KEY'] = 'spinwinny-secret-key-2025'

# Enable CORS for frontend-backend communication
CORS(app, origins=['http://localhost:5173', 'http://localhost:5174', 'https://*.spinwinny.com'])

# Register blueprints
app.register_blueprint(wheels_bp)
app.register_blueprint(users_bp)
app.register_blueprint(data_integration_bp)

# Database configuration
app.config['SQLALCHEMY_DATABASE_URI'] = f"sqlite:///{os.path.join(os.path.dirname(__file__), 'database', 'app.db')}"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

# Create database tables
with app.app_context():
    db.create_all()

# Analytics endpoints
@app.route('/api/analytics/record', methods=['POST'])
def record_analytics_event():
    """Record analytics event"""
    try:
        data = request.get_json()
        event_type = data.get('event_type')
        event_data = data.get('event_data', {})
        wheel_id = data.get('wheel_id')
        user_id = data.get('user_id')
        session_id = data.get('session_id')
        
        # Get client info
        ip_address = request.remote_addr
        user_agent = request.headers.get('User-Agent', '')
        
        success = analytics_service.record_event(
            event_type, event_data, wheel_id, user_id, session_id,
            ip_address, user_agent
        )
        
        return jsonify({'success': success})
        
    except Exception as e:
        logger.error(f"Error recording analytics event: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/analytics/wheel/<wheel_id>', methods=['GET'])
def get_wheel_analytics(wheel_id):
    """Get analytics for specific wheel"""
    try:
        days = request.args.get('days', 30, type=int)
        analytics = analytics_service.get_wheel_analytics(wheel_id, days)
        return jsonify(analytics)
        
    except Exception as e:
        logger.error(f"Error getting wheel analytics: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/global', methods=['GET'])
def get_global_analytics():
    """Get global platform analytics"""
    try:
        days = request.args.get('days', 30, type=int)
        analytics = analytics_service.get_global_analytics(days)
        return jsonify(analytics)
        
    except Exception as e:
        logger.error(f"Error getting global analytics: {e}")
        return jsonify({'error': str(e)}), 500

# Embed route for iframe embedding
@app.route('/embed/<share_id>')
def embed_wheel(share_id):
    """Serve embedded wheel"""
    embed_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Spinwinny Wheel</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body {{ margin: 0; padding: 0; overflow: hidden; }}
            iframe {{ width: 100%; height: 100vh; border: none; }}
        </style>
    </head>
    <body>
        <iframe src="/wheel/{share_id}?embed=true" width="100%" height="100%" frameborder="0"></iframe>
    </body>
    </html>
    """
    return render_template_string(embed_html)

# API health check
@app.route('/api/health')
def health_check():
    return {
        'success': True, 
        'message': 'Spinwinny API is running',
        'timestamp': datetime.now().isoformat(),
        'services': {
            'database': True,
            'analytics': True,
            'data_integration': True
        }
    }

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    static_folder_path = app.static_folder
    if static_folder_path is None:
        return "Static folder not configured", 404

    if path != "" and os.path.exists(os.path.join(static_folder_path, path)):
        return send_from_directory(static_folder_path, path)
    else:
        index_path = os.path.join(static_folder_path, 'index.html')
        if os.path.exists(index_path):
            return send_from_directory(static_folder_path, 'index.html')
        else:
            return "index.html not found", 404

# Error handlers
@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Endpoint not found'}), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({'error': 'Internal server error'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
