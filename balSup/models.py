from django.db import models
 
class BalSupModel(models.Model):
    periodo = models.SmallIntegerField()
    mes = models.SmallIntegerField()
    entidad_RS = models.CharField(max_length=150)
    puc_codigo = models.CharField(max_length=128)
    saldo = models.DecimalField(max_digits=18, decimal_places=2)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "bal_sup"
        ordering = ['-created_at']