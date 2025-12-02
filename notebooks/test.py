"""
THEORETICAL BEST TIME - CONCEPT EXPLIQUÉ
=========================================

PROBLÈME:
---------
Segment A: 1000 efforts → best_time = 180s (fiable, beaucoup d'élite sont passés)
Segment B: 30 efforts   → best_time = 200s (biaisé, peu d'élite)

Question: Est-ce que B est vraiment plus lent? Ou juste moins populaire?

SOLUTION: RÉGRESSION SUPERVISÉE (PAS DE LABELS TECHNIQUES!)
------------------------------------------------------------
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score

# Simuler un dataset pour comprendre
np.random.seed(42)

def simulate_segments(n=2000):
    """
    Simule des segments pour illustrer le concept
    """
    data = []
    
    for i in range(n):
        # Caractéristiques physiques (SANS technicité)
        distance = np.random.uniform(500, 5000)  # mètres
        elevation = np.random.uniform(0, 500)     # mètres
        
        # Nombre d'efforts (popularité)
        effort_count = np.random.choice(
            [30, 50, 100, 200, 500, 1000, 2000],
            p=[0.3, 0.25, 0.2, 0.15, 0.07, 0.02, 0.01]  # Beaucoup de rares
        )
        
        # === TEMPS THÉORIQUE (ce qu'on veut apprendre) ===
        # Vitesse de base: 5 m/s
        # Pénalité élévation: 3s par mètre
        theoretical_time = (distance / 5.0) + (elevation * 3.0)
        
        # === TEMPS OBSERVÉ (biaisé par la popularité) ===
        # Plus il y a d'efforts, plus on est proche du théorique
        # Peu d'efforts → temps plus lent (pas assez d'élite)
        sampling_bias = np.random.uniform(1.0, 1.5 - (effort_count / 2500))
        observed_time = theoretical_time * sampling_bias
        
        data.append({
            'segment_id': i,
            'distance': distance,
            'elevation': elevation,
            'effort_count': effort_count,
            'theoretical_time': theoretical_time,  # ← On ne connaît PAS ça
            'observed_time': observed_time          # ← On a SEULEMENT ça
        })
    
    return pd.DataFrame(data)

df = simulate_segments(2000)

print("="*70)
print("DATASET SIMULÉ")
print("="*70)
print(f"Total segments: {len(df)}")
print(f"\nCe qu'on a:")
print(f"  ✓ distance")
print(f"  ✓ elevation") 
print(f"  ✓ observed_time (best_time de Strava)")
print(f"  ✓ effort_count")
print(f"\nCe qu'on veut estimer:")
print(f"  ? theoretical_time (temps si segment populaire)")
print("="*70)


# %% ========== ÉTAPE 1: IDENTIFIER LES SEGMENTS "FIABLES" ==========

print("\n" + "="*70)
print("ÉTAPE 1: IDENTIFIER LE GROUND TRUTH")
print("="*70)

# Segments avec beaucoup d'efforts = temps fiable
threshold = 500
reliable_segments = df[df['effort_count'] >= threshold]
unreliable_segments = df[df['effort_count'] < threshold]

print(f"\nSeuil choisi: {threshold} efforts")
print(f"  Segments fiables: {len(reliable_segments)} ({len(reliable_segments)/len(df)*100:.1f}%)")
print(f"  Segments biaisés: {len(unreliable_segments)} ({len(unreliable_segments)/len(df)*100:.1f}%)")

# Visualiser le biais
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Biais vs effort count
df['bias'] = (df['observed_time'] - df['theoretical_time']) / df['theoretical_time']

axes[0].scatter(df['effort_count'], df['bias'], alpha=0.6)
axes[0].axhline(0, color='red', linestyle='--', lw=2, label='Pas de biais')
axes[0].axvline(threshold, color='green', linestyle='--', lw=2, label=f'Seuil ({threshold})')
axes[0].set_xlabel('Effort Count')
axes[0].set_ylabel('Biais (observed/theoretical - 1)')
axes[0].set_title('Biais de Sampling vs Popularité')
axes[0].set_xscale('log')
axes[0].legend()
axes[0].grid(alpha=0.3)

# Distribution du biais
axes[1].hist(reliable_segments['bias'], bins=30, alpha=0.7, label='Fiables (≥500)')
axes[1].hist(unreliable_segments['bias'], bins=30, alpha=0.7, label='Biaisés (<500)')
axes[1].axvline(0, color='red', linestyle='--', lw=2)
axes[1].set_xlabel('Biais')
axes[1].set_ylabel('Count')
axes[1].set_title('Distribution du Biais')
axes[1].legend()
axes[1].grid(alpha=0.3)

plt.tight_layout()
plt.show()

print(f"\n📊 Biais moyen:")
print(f"  Segments fiables: {reliable_segments['bias'].mean():+.3f} (proche de 0 ✓)")
print(f"  Segments biaisés: {unreliable_segments['bias'].mean():+.3f} (positif = trop lent)")


# %% ========== ÉTAPE 2: ENTRAÎNER LE MODÈLE (RÉGRESSION) ==========

print("\n" + "="*70)
print("ÉTAPE 2: RÉGRESSION SUPERVISÉE")
print("="*70)

# IMPORTANT: On utilise observed_time comme TARGET pour les segments fiables
# Car pour eux: observed_time ≈ theoretical_time

X_train = reliable_segments[['distance', 'elevation']].values
y_train = reliable_segments['observed_time'].values  # ← PAS DE LABELS! Juste le temps observé

X_test = unreliable_segments[['distance', 'elevation']].values
y_test = unreliable_segments['theoretical_time'].values  # Pour évaluer (on fait semblant)

print(f"\nEntraînement:")
print(f"  Features (X): distance, elevation")
print(f"  Target (y): observed_time des segments fiables")
print(f"  Hypothèse: Pour segments fiables, observed_time ≈ theoretical_time")

# Entraîner
model = GradientBoostingRegressor(n_estimators=100, max_depth=4, random_state=42)
model.fit(X_train, y_train)

# Prédire
df['predicted_time'] = model.predict(df[['distance', 'elevation']].values)

mae_train = mean_absolute_error(y_train, model.predict(X_train))
r2_train = r2_score(y_train, model.predict(X_train))

print(f"\n✓ Modèle entraîné")
print(f"  MAE (train): {mae_train:.1f}s")
print(f"  R² (train): {r2_train:.3f}")


# %% ========== ÉTAPE 3: ÉVALUER LA QUALITÉ ==========

print("\n" + "="*70)
print("ÉTAPE 3: ÉVALUATION")
print("="*70)

# Pour les segments fiables: predicted ≈ observed (espéré)
reliable_pred = df[df['effort_count'] >= threshold]['predicted_time']
reliable_obs = df[df['effort_count'] >= threshold]['observed_time']
mae_reliable = mean_absolute_error(reliable_obs, reliable_pred)

# Pour les segments biaisés: predicted < observed (correction du biais)
unreliable_pred = df[df['effort_count'] < threshold]['predicted_time']
unreliable_obs = df[df['effort_count'] < threshold]['observed_time']
mae_unreliable = mean_absolute_error(unreliable_obs, unreliable_pred)

print(f"\nPerformance:")
print(f"  MAE sur segments fiables: {mae_reliable:.1f}s")
print(f"  MAE sur segments biaisés: {mae_unreliable:.1f}s")

# Visualiser
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Segments fiables
axes[0].scatter(reliable_obs, reliable_pred, alpha=0.6, s=20)
axes[0].plot([0, reliable_obs.max()], [0, reliable_obs.max()], 'r--', lw=2, label='Perfect')
axes[0].set_xlabel('Observed Time (s)')
axes[0].set_ylabel('Predicted Time (s)')
axes[0].set_title(f'Segments Fiables (n={len(reliable_obs)})')
axes[0].legend()
axes[0].grid(alpha=0.3)

# Segments biaisés
axes[1].scatter(unreliable_obs, unreliable_pred, alpha=0.6, s=20, color='orange')
axes[1].plot([0, unreliable_obs.max()], [0, unreliable_obs.max()], 'r--', lw=2, label='Perfect')
axes[1].set_xlabel('Observed Time (s)')
axes[1].set_ylabel('Predicted Time (s)')
axes[1].set_title(f'Segments Biaisés (n={len(unreliable_obs)})')
axes[1].legend()
axes[1].grid(alpha=0.3)

plt.tight_layout()
plt.show()


# %% ========== ÉTAPE 4: CORRECTION DU BIAIS ==========

print("\n" + "="*70)
print("ÉTAPE 4: CORRECTION DU BIAIS")
print("="*70)

# Calculer la correction
df['correction'] = df['predicted_time'] - df['observed_time']
df['correction_pct'] = (df['correction'] / df['observed_time']) * 100

# Analyser la correction par popularité
print(f"\nCorrection moyenne par tranche d'efforts:")
effort_bins = [0, 50, 100, 200, 500, 1000, 10000]
for i in range(len(effort_bins)-1):
    mask = (df['effort_count'] >= effort_bins[i]) & (df['effort_count'] < effort_bins[i+1])
    if mask.sum() > 0:
        avg_correction = df[mask]['correction'].mean()
        avg_pct = df[mask]['correction_pct'].mean()
        print(f"  {effort_bins[i]:4d}-{effort_bins[i+1]:4d} efforts: {avg_correction:+6.1f}s ({avg_pct:+5.1f}%)")

# Visualiser
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Correction vs effort count
axes[0].scatter(df['effort_count'], df['correction'], alpha=0.6)
axes[0].axhline(0, color='red', linestyle='--', lw=2)
axes[0].axvline(threshold, color='green', linestyle='--', lw=2, alpha=0.5)
axes[0].set_xlabel('Effort Count')
axes[0].set_ylabel('Correction (predicted - observed) [s]')
axes[0].set_title('Correction du Biais')
axes[0].set_xscale('log')
axes[0].grid(alpha=0.3)

# Distribution des corrections
axes[1].hist(df[df['effort_count'] < threshold]['correction'], 
            bins=50, alpha=0.7, edgecolor='black')
axes[1].axvline(0, color='red', linestyle='--', lw=2)
axes[1].set_xlabel('Correction (s)')
axes[1].set_ylabel('Count')
axes[1].set_title('Distribution des Corrections (segments biaisés)')
axes[1].grid(alpha=0.3)

plt.tight_layout()
plt.show()

# Statistiques finales
print(f"\n📊 Résumé:")
print(f"  Segments avec correction <0 (plus rapides): {(df['correction'] < 0).sum()}")
print(f"  Segments avec correction >0 (plus lents): {(df['correction'] > 0).sum()}")
print(f"  Correction médiane (biaisés): {df[df['effort_count'] < threshold]['correction'].median():.1f}s")


# %% ========== ÉTAPE 5: VALIDATION (SI ON AVAIT LE VRAI THÉORIQUE) ==========

print("\n" + "="*70)
print("ÉTAPE 5: VALIDATION (simulation - vous n'avez pas ça en vrai)")
print("="*70)

# Dans la simulation, on a le vrai theoretical_time
# En vrai, vous ne l'aurez JAMAIS

mae_vs_true = mean_absolute_error(df['theoretical_time'], df['predicted_time'])
r2_vs_true = r2_score(df['theoretical_time'], df['predicted_time'])

print(f"\nPrécision vs temps théorique réel:")
print(f"  MAE: {mae_vs_true:.1f}s")
print(f"  R²: {r2_vs_true:.3f}")

# Comparaison: observed vs predicted
fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Observed time (biaisé)
axes[0].scatter(df['theoretical_time'], df['observed_time'], 
               c=df['effort_count'], alpha=0.6, s=20, cmap='viridis')
axes[0].plot([0, df['theoretical_time'].max()], [0, df['theoretical_time'].max()], 
            'r--', lw=2)
axes[0].set_xlabel('True Theoretical Time (s)')
axes[0].set_ylabel('Observed Time (s)')
axes[0].set_title('Observed (biaisé par popularité)')
cbar = plt.colorbar(axes[0].collections[0], ax=axes[0])
cbar.set_label('Effort Count')

# Predicted time (corrigé)
axes[1].scatter(df['theoretical_time'], df['predicted_time'], 
               alpha=0.6, s=20, color='green')
axes[1].plot([0, df['theoretical_time'].max()], [0, df['theoretical_time'].max()], 
            'r--', lw=2)
axes[1].set_xlabel('True Theoretical Time (s)')
axes[1].set_ylabel('Predicted Time (s)')
axes[1].set_title('Predicted (corrigé)')

plt.tight_layout()
plt.show()


# %% ========== CONCLUSION ==========

print("\n" + "="*80)
print("CONCLUSION: CE QU'ON FAIT VRAIMENT")
print("="*80)
print("""
1. HYPOTHÈSE CLÉ:
   Segments populaires (500+ efforts) → observed_time ≈ theoretical_time
   Car beaucoup d'athlètes d'élite sont passés

2. ENTRAÎNEMENT:
   X = [distance, elevation]
   y = observed_time (SEULEMENT des segments populaires)
   
   On apprend: f(distance, elevation) → temps
   SANS aucune information sur la technicité!

3. PRÉDICTION:
   Pour TOUS les segments: predicted_time = f(distance, elevation)
   
4. INTERPRÉTATION:
   - Si predicted < observed → Segment sous-estimé (pas assez d'élite)
   - Si predicted ≈ observed → Segment bien estimé
   - Si predicted > observed → Impossible* (sauf si technicité)
   
   *En théorie, predicted devrait toujours être ≤ observed
   
5. UTILISATION:
   theoretical_best_time = predicted_time
   → Servira de baseline pour détecter la technicité!

AUCUN LABEL DE TECHNICITÉ N'EST NÉCESSAIRE À CETTE ÉTAPE!
""")

print("\n📊 Dans votre cas:")
print(f"  Total segments: ~3000")
print(f"  Segments >500 efforts: ~??? (à vérifier)")
print(f"  → Utilisez ces segments comme ground truth")
print(f"  → Prédisez sur les 3000")
print(f"  → Vous aurez 'theoretical_best_time' pour tous")
print("="*80)