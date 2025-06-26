from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from app.schemas import user as schema
from app.models import user as model
from app.database import SessionLocal
from app.services.kafka_producer import send_kafka_message

router = APIRouter(prefix="/users", tags=["Users"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/", response_model=schema.UserOut)
def create_user(data: schema.UserCreate, db: Session = Depends(get_db)):
    user = model.User(name=data.name, email=data.email)
    db.add(user)
    db.commit()
    db.refresh(user)

    send_kafka_message("user-topic", f"New user created: {user.email}")
    return user
