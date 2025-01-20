from django.db import models
from django.utils import timezone

from django.contrib.auth.models import AbstractBaseUser, BaseUserManager, PermissionsMixin

class UserManager(BaseUserManager):
    def create_user(self, username, email, password=None, is_staff=False, **kwargs):
        if username is None:
            raise TypeError('Users must have a username.')
        if email is None:
            raise TypeError('Users must have an email.')
        user = self.model(username=username, email=self.normalize_email(email), is_staff=is_staff)
        user.set_password(password)
        user.save(using=self._db)
        return user

    def create_superuser(self, username, email, password):
        if password is None:
            raise TypeError('Superusers must have a password.')
        if email is None:
            raise TypeError('Superusers must have an email.')
        if username is None:
            raise TypeError('Superusers must have a username.')
        user = self.create_user(username, email, password, is_staff=True, is_superuser=True)
        return user

class User(AbstractBaseUser, PermissionsMixin):
    username = models.CharField(db_index=True, max_length=255)
    email = models.EmailField(db_index=True, unique=True, null=True, blank=True)
    is_active = models.BooleanField(default=True)
    is_staff = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    updated_at = models.DateTimeField(auto_now_add=True, null=True)

    USERNAME_FIELD = 'email'
    REQUIRED_FIELDS = ['username']

    objects = UserManager()

    def __str__(self):
        return f"{self.email}"

class Subscription(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='subscriptions')
    fecha_inicio_suscripcion = models.DateField()
    fecha_final_suscripcion = models.DateField()
    is_active = models.BooleanField(default=True)

    def save(self, *args, **kwargs):
        self.check_subscription_status()

        super().save(*args, **kwargs)
        self.update_user_status()

    def check_subscription_status(self):
        today = timezone.now().date()

        if self.fecha_final_suscripcion < today:
            self.is_active = False

    def update_user_status(self):
        if not self.user.is_staff:
            if not self.user.subscriptions.filter(is_active=True).exists():
                self.user.is_active = False
            else:
                self.user.is_active = True
            self.user.save()

    def __str__(self):
        return f"Subscription for {self.user.email} from {self.fecha_inicio_suscripcion} to {self.fecha_final_suscripcion}"
