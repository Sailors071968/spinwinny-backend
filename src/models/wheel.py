from datetime import datetime
from src.database import db

class Wheel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    entries = db.Column(db.Text, nullable=False)  # JSON string
    settings = db.Column(db.Text, nullable=False)  # JSON string
    share_id = db.Column(db.String(50), unique=True, nullable=True)
    is_public = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    user_id = db.Column(db.String(100), nullable=True)  # For authenticated users
    view_count = db.Column(db.Integer, default=0)
    spin_count = db.Column(db.Integer, default=0)
    category = db.Column(db.String(50), default='general')
    tags = db.Column(db.String(500), nullable=True)
    
    def to_dict(self):
        return {
            'id': self.id,
            'title': self.title,
            'entries': self.entries,
            'settings': self.settings,
            'share_id': self.share_id,
            'is_public': self.is_public,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat(),
            'user_id': self.user_id,
            'view_count': self.view_count,
            'spin_count': self.spin_count,
            'category': self.category,
            'tags': self.tags
        }

class SpinResult(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    wheel_id = db.Column(db.Integer, db.ForeignKey('wheel.id'), nullable=False)
    winner = db.Column(db.String(200), nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    user_id = db.Column(db.String(100), nullable=True)
    session_id = db.Column(db.String(100), nullable=True)
    
    def to_dict(self):
        return {
            'id': self.id,
            'wheel_id': self.wheel_id,
            'winner': self.winner,
            'timestamp': self.timestamp.isoformat(),
            'user_id': self.user_id,
            'session_id': self.session_id
        }

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(100), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=True)
    display_name = db.Column(db.String(100), nullable=True)
    subscription_tier = db.Column(db.String(20), default='free')
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_active = db.Column(db.DateTime, default=datetime.utcnow)
    wheel_count = db.Column(db.Integer, default=0)
    total_spins = db.Column(db.Integer, default=0)
    
    def to_dict(self):
        return {
            'id': self.id,
            'user_id': self.user_id,
            'email': self.email,
            'display_name': self.display_name,
            'subscription_tier': self.subscription_tier,
            'created_at': self.created_at.isoformat(),
            'last_active': self.last_active.isoformat(),
            'wheel_count': self.wheel_count,
            'total_spins': self.total_spins
        }

