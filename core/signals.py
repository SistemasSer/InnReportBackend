from django.db.models.signals import post_save
from django.dispatch import receiver
from django.utils import timezone
from .user.models import Subscription

@receiver(post_save, sender=Subscription)
def check_subscription_status(sender, instance, **kwargs):
    today = timezone.now().date()

    if instance.fecha_final_suscripcion < today and instance.is_active:
        instance.is_active = False
        instance.save(update_fields=['is_active']) 