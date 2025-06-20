from django.db import models
 
class PucSupModel(models.Model):
    Codigo = models.CharField(max_length=128)
    Descripcion = models.CharField(max_length=300)
    Agrupa = models.CharField(max_length=10, null=True, default=True)
    CreditoRiesgo = models.SmallIntegerField()
    created_at = models.DateTimeField(auto_now_add=True, null=True)
    updated_at = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        db_table = "puc_sup"
        ordering = ['Codigo']