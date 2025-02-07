from rest_framework import serializers

class ExchangeRateDetailSerializer(serializers.Serializer):
    date = serializers.DateField()
    close = serializers.FloatField()

class ExchangeRateSerializer(serializers.Serializer):
    currency = serializers.CharField()
    current = ExchangeRateDetailSerializer()
    previous = ExchangeRateDetailSerializer()

class ExchangeRateRawMaterialsDetailSerializer(serializers.Serializer):
    date = serializers.DateField()
    close = serializers.FloatField()

class ExchangeRateRawMaterialsSerializer(serializers.Serializer):
    rawMaterial = serializers.CharField()
    current = ExchangeRateRawMaterialsDetailSerializer(required=False)
    previous = ExchangeRateRawMaterialsDetailSerializer(required=False)
    error = serializers.CharField(required=False)

class CombinedExchangeRateDetailSerializer(serializers.Serializer):
    date = serializers.DateField()
    close = serializers.FloatField()

class CombinedExchangeRateSerializer(serializers.Serializer):
    currency = serializers.CharField(required=False)
    rawMaterial = serializers.CharField(required=False)
    materialName = serializers.CharField(required=False)
    unit = serializers.CharField(required=False)
    current = CombinedExchangeRateDetailSerializer(required=False)
    previous = CombinedExchangeRateDetailSerializer(required=False)
    error = serializers.CharField(required=False)