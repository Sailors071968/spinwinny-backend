from flask import Blueprint, request, jsonify
from src.database import db
from src.models.wheel import User, Wheel, SpinResult
import uuid
from datetime import datetime

users_bp = Blueprint('users', __name__)

@users_bp.route('/api/users', methods=['POST'])
def create_user():
    """Create or get existing user"""
    try:
        data = request.get_json()
        
        # Check if user already exists
        existing_user = User.query.filter_by(user_id=data.get('user_id')).first()
        if existing_user:
            existing_user.last_active = datetime.utcnow()
            if data.get('email'):
                existing_user.email = data.get('email')
            if data.get('display_name'):
                existing_user.display_name = data.get('display_name')
            db.session.commit()
            return jsonify({
                'success': True,
                'user': existing_user.to_dict(),
                'is_new': False
            })
        
        # Create new user
        user = User(
            user_id=data.get('user_id') or str(uuid.uuid4()),
            email=data.get('email'),
            display_name=data.get('display_name'),
            subscription_tier=data.get('subscription_tier', 'free')
        )
        
        db.session.add(user)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'user': user.to_dict(),
            'is_new': True
        }), 201
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@users_bp.route('/api/users/<user_id>', methods=['GET'])
def get_user(user_id):
    """Get user profile"""
    try:
        user = User.query.filter_by(user_id=user_id).first()
        if not user:
            return jsonify({'success': False, 'error': 'User not found'}), 404
        
        return jsonify({
            'success': True,
            'user': user.to_dict()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@users_bp.route('/api/users/<user_id>', methods=['PUT'])
def update_user(user_id):
    """Update user profile"""
    try:
        data = request.get_json()
        user = User.query.filter_by(user_id=user_id).first()
        
        if not user:
            return jsonify({'success': False, 'error': 'User not found'}), 404
        
        user.email = data.get('email', user.email)
        user.display_name = data.get('display_name', user.display_name)
        user.subscription_tier = data.get('subscription_tier', user.subscription_tier)
        user.last_active = datetime.utcnow()
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'user': user.to_dict()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@users_bp.route('/api/users/<user_id>/stats', methods=['GET'])
def get_user_stats(user_id):
    """Get user statistics"""
    try:
        user = User.query.filter_by(user_id=user_id).first()
        if not user:
            return jsonify({'success': False, 'error': 'User not found'}), 404
        
        # Get additional stats
        total_views = db.session.query(db.func.sum(Wheel.view_count)).filter_by(user_id=user_id).scalar() or 0
        total_wheel_spins = db.session.query(db.func.sum(Wheel.spin_count)).filter_by(user_id=user_id).scalar() or 0
        public_wheels = Wheel.query.filter_by(user_id=user_id, is_public=True).count()
        private_wheels = Wheel.query.filter_by(user_id=user_id, is_public=False).count()
        
        # Recent activity
        recent_spins = SpinResult.query.filter_by(user_id=user_id).order_by(
            SpinResult.timestamp.desc()
        ).limit(10).all()
        
        return jsonify({
            'success': True,
            'stats': {
                'total_wheels': user.wheel_count,
                'total_spins': user.total_spins,
                'total_views': total_views,
                'total_wheel_spins': total_wheel_spins,
                'public_wheels': public_wheels,
                'private_wheels': private_wheels,
                'subscription_tier': user.subscription_tier,
                'member_since': user.created_at.isoformat(),
                'last_active': user.last_active.isoformat(),
                'recent_spins': [spin.to_dict() for spin in recent_spins]
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@users_bp.route('/api/users/<user_id>/subscription', methods=['PUT'])
def update_subscription(user_id):
    """Update user subscription tier"""
    try:
        data = request.get_json()
        user = User.query.filter_by(user_id=user_id).first()
        
        if not user:
            return jsonify({'success': False, 'error': 'User not found'}), 404
        
        valid_tiers = ['free', 'premium', 'enterprise']
        new_tier = data.get('subscription_tier')
        
        if new_tier not in valid_tiers:
            return jsonify({'success': False, 'error': 'Invalid subscription tier'}), 400
        
        user.subscription_tier = new_tier
        user.last_active = datetime.utcnow()
        
        db.session.commit()
        
        return jsonify({
            'success': True,
            'user': user.to_dict()
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@users_bp.route('/api/auth/guest', methods=['POST'])
def create_guest_session():
    """Create a guest session"""
    try:
        guest_id = f"guest_{uuid.uuid4().hex[:8]}"
        
        # Create temporary guest user
        guest_user = User(
            user_id=guest_id,
            display_name=f"Guest {guest_id[-4:]}",
            subscription_tier='free'
        )
        
        db.session.add(guest_user)
        db.session.commit()
        
        return jsonify({
            'success': True,
            'user': guest_user.to_dict(),
            'session_id': guest_id
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

