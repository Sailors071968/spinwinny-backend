from flask import Blueprint, request, jsonify
from src.database import db
from src.models.wheel import Wheel, SpinResult, User
import json
import uuid
import string
import random
from datetime import datetime

wheels_bp = Blueprint('wheels', __name__)

def generate_share_id():
    """Generate a unique share ID for wheels"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=8))

@wheels_bp.route('/api/wheels', methods=['POST'])
def create_wheel():
    """Create a new wheel"""
    try:
        data = request.get_json()
        
        # Generate unique share ID if wheel is public
        share_id = None
        if data.get('is_public', False):
            share_id = generate_share_id()
            # Ensure uniqueness
            while Wheel.query.filter_by(share_id=share_id).first():
                share_id = generate_share_id()
        
        wheel = Wheel(
            title=data.get('title', 'Untitled Wheel'),
            entries=json.dumps(data.get('entries', [])),
            settings=json.dumps(data.get('settings', {})),
            share_id=share_id,
            is_public=data.get('is_public', False),
            user_id=data.get('user_id'),
            category=data.get('category', 'general'),
            tags=data.get('tags', '')
        )
        
        db.session.add(wheel)
        db.session.commit()
        
        # Update user wheel count if user exists
        if wheel.user_id:
            user = User.query.filter_by(user_id=wheel.user_id).first()
            if user:
                user.wheel_count += 1
                db.session.commit()
        
        return jsonify({
            'success': True,
            'wheel': wheel.to_dict(),
            'share_url': f'/wheel/{share_id}' if share_id else None
        }), 201
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@wheels_bp.route('/api/wheels/<share_id>', methods=['GET'])
def get_wheel(share_id):
    """Get a wheel by share ID"""
    try:
        wheel = Wheel.query.filter_by(share_id=share_id).first()
        if not wheel:
            return jsonify({'success': False, 'error': 'Wheel not found'}), 404
        
        # Increment view count
        wheel.view_count += 1
        db.session.commit()
        
        return jsonify({
            'success': True,
            'wheel': wheel.to_dict()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@wheels_bp.route('/api/wheels/<share_id>/spin', methods=['POST'])
def record_spin(share_id):
    """Record a spin result"""
    try:
        data = request.get_json()
        wheel = Wheel.query.filter_by(share_id=share_id).first()
        
        if not wheel:
            return jsonify({'success': False, 'error': 'Wheel not found'}), 404
        
        # Record the spin result
        spin_result = SpinResult(
            wheel_id=wheel.id,
            winner=data.get('winner', ''),
            user_id=data.get('user_id'),
            session_id=data.get('session_id')
        )
        
        # Update wheel spin count
        wheel.spin_count += 1
        
        # Update user total spins if user exists
        if data.get('user_id'):
            user = User.query.filter_by(user_id=data.get('user_id')).first()
            if user:
                user.total_spins += 1
                user.last_active = datetime.utcnow()
        
        db.session.add(spin_result)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'spin_result': spin_result.to_dict()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@wheels_bp.route('/api/gallery', methods=['GET'])
def get_gallery():
    """Get public wheels for gallery"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 12, type=int)
        category = request.args.get('category', 'all')
        search = request.args.get('search', '')
        
        query = Wheel.query.filter_by(is_public=True)
        
        if category != 'all':
            query = query.filter_by(category=category)
        
        if search:
            query = query.filter(Wheel.title.contains(search))
        
        # Order by popularity (view count + spin count)
        wheels = query.order_by((Wheel.view_count + Wheel.spin_count).desc()).paginate(
            page=page, per_page=per_page, error_out=False
        )
        
        return jsonify({
            'success': True,
            'wheels': [wheel.to_dict() for wheel in wheels.items],
            'total': wheels.total,
            'pages': wheels.pages,
            'current_page': page,
            'has_next': wheels.has_next,
            'has_prev': wheels.has_prev
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@wheels_bp.route('/api/gallery/categories', methods=['GET'])
def get_categories():
    """Get available categories"""
    try:
        categories = db.session.query(Wheel.category).filter_by(is_public=True).distinct().all()
        category_list = [cat[0] for cat in categories if cat[0]]
        
        return jsonify({
            'success': True,
            'categories': ['all'] + sorted(category_list)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@wheels_bp.route('/api/wheels/<share_id>/embed', methods=['GET'])
def get_embed_code(share_id):
    """Get embed code for a wheel"""
    try:
        wheel = Wheel.query.filter_by(share_id=share_id).first()
        if not wheel or not wheel.is_public:
            return jsonify({'success': False, 'error': 'Wheel not found or not public'}), 404
        
        embed_url = f"{request.host_url}embed/{share_id}"
        iframe_code = f'<iframe src="{embed_url}" width="600" height="600" frameborder="0"></iframe>'
        script_code = f'<script src="{request.host_url}embed/{share_id}.js"></script>'
        
        return jsonify({
            'success': True,
            'embed_url': embed_url,
            'iframe_code': iframe_code,
            'script_code': script_code
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@wheels_bp.route('/api/user/<user_id>/wheels', methods=['GET'])
def get_user_wheels(user_id):
    """Get wheels for a specific user"""
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)
        
        wheels = Wheel.query.filter_by(user_id=user_id).order_by(
            Wheel.updated_at.desc()
        ).paginate(page=page, per_page=per_page, error_out=False)
        
        return jsonify({
            'success': True,
            'wheels': [wheel.to_dict() for wheel in wheels.items],
            'total': wheels.total,
            'pages': wheels.pages,
            'current_page': page
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@wheels_bp.route('/api/wheels/<int:wheel_id>', methods=['PUT'])
def update_wheel(wheel_id):
    """Update a wheel"""
    try:
        data = request.get_json()
        wheel = Wheel.query.get_or_404(wheel_id)
        
        # Check if user owns this wheel
        if wheel.user_id != data.get('user_id'):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403
        
        wheel.title = data.get('title', wheel.title)
        wheel.entries = json.dumps(data.get('entries', json.loads(wheel.entries)))
        wheel.settings = json.dumps(data.get('settings', json.loads(wheel.settings)))
        wheel.is_public = data.get('is_public', wheel.is_public)
        wheel.category = data.get('category', wheel.category)
        wheel.tags = data.get('tags', wheel.tags)
        wheel.updated_at = datetime.utcnow()
        
        # Generate share ID if making public for first time
        if wheel.is_public and not wheel.share_id:
            wheel.share_id = generate_share_id()
            while Wheel.query.filter_by(share_id=wheel.share_id).first():
                wheel.share_id = generate_share_id()
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'wheel': wheel.to_dict()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@wheels_bp.route('/api/wheels/<int:wheel_id>', methods=['DELETE'])
def delete_wheel(wheel_id):
    """Delete a wheel"""
    try:
        data = request.get_json()
        wheel = Wheel.query.get_or_404(wheel_id)
        
        # Check if user owns this wheel
        if wheel.user_id != data.get('user_id'):
            return jsonify({'success': False, 'error': 'Unauthorized'}), 403
        
        # Delete associated spin results
        SpinResult.query.filter_by(wheel_id=wheel.id).delete()
        
        # Update user wheel count
        if wheel.user_id:
            user = User.query.filter_by(user_id=wheel.user_id).first()
            if user and user.wheel_count > 0:
                user.wheel_count -= 1
        
        db.session.delete(wheel)
        db.session.commit()
        
        return jsonify({'success': True})
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

