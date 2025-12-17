import numpy as np
import pickle, sys, os
from scipy.optimize import fsolve
from scipy.interpolate import interp1d, RegularGridInterpolator
from pathlib import Path
current_script_path = Path(__file__).resolve()
repo_root = current_script_path.parents[2] 
sys.path.append(str(repo_root))

class RideLookUpTableGenerator:
        
    # ============================================================================
    # POWER PROFILE - Modèle réaliste basé sur Critical Power
    # ============================================================================
    P_inf = 250  # Puissance asymptotique (W)
    AWC = 2900 # W.s**a
    a = 0.5  # Exposant ajusté pour mieux correspondre aux données réelles

    def power_profile(self, t, P_inf=P_inf, AWC=AWC, a=a):
        """Profil de puissance maximal en fonction du temps (en secondes)"""

        if t <= 0:
            return 0
        elif t < 15:
            return P_inf + AWC / 15**a  # Puissance maximale pour très court terme
        else:
            return P_inf + AWC / t**a


    # ============================================================================
    # MODÈLE PHYSIQUE - Puissance → Vitesse
    # ============================================================================

    def build_power_model(self, power=250, mass_total=75):
        """
        Construit un modèle qui convertit grade → vitesse pour une puissance donnée.
        
        Équation de puissance cycliste:
        P = P_gravity + P_rolling + P_air
        P = m*g*v*sin(θ) + Crr*m*g*v*cos(θ) + 0.5*ρ*CdA*v³
        
        Args:
            power: puissance en Watts
            mass_total: masse totale (cycliste + vélo) en kg
        
        Returns:
            function: interpolateur grade(%) → vitesse(m/s)
        """
        g = 9.81  # m/s²
        Crr = 0.004  # Coefficient de résistance au roulement (route lisse)
        rho = 1.225  # kg/m³ (densité de l'air au niveau de la mer)
        CdA = 0.32  # m² (traînée aérodynamique, position route)
        
        def power_equation(v, grade_percent):
            """Équation à résoudre: P_total - P_disponible = 0"""
            theta = np.arctan(grade_percent / 100)
            
            P_gravity = mass_total * g * v * np.sin(theta)
            P_rolling = Crr * mass_total * g * v * np.cos(theta)
            P_air = 0.5 * rho * CdA * v**3
            
            return P_gravity + P_rolling + P_air - power
        
        # Générer la courbe grade → vitesse
        grades = np.linspace(-10, 20, 301)
        velocities = []
        
        for grade in grades:
            # Estimation initiale adaptative
            if grade < -5:
                v0 = 20  # Descente rapide
            elif grade < 5:
                v0 = 10  # Plat/faux-plat
            else:
                v0 = 5   # Montée
            
            try:
                v_solution = fsolve(power_equation, v0, args=(grade,))[0]
                if v_solution > 0.5:  # Vitesse minimale réaliste
                    velocities.append(v_solution)
                else:
                    velocities.append(np.nan)
            except:
                velocities.append(np.nan)
        
        # Créer l'interpolateur
        velocities = np.array(velocities)
        valid_mask = ~np.isnan(velocities)
        
        interpolator = interp1d(
            grades[valid_mask], 
            velocities[valid_mask], 
            kind='cubic', 
            bounds_error=False, 
            fill_value='extrapolate'
        )
        
        return interpolator


    # ============================================================================
    # CONSTRUCTION DE LA LOOKUP TABLE 3D
    # ============================================================================

    def build_lookup_table_3d(
        self,
        section_distance_range=(0.05, 10.0),  # km (distance d'une section)
        section_distance_step=0.05,            # tous les 50m
        section_grade_range=(-10, 20),         # %
        section_grade_step=0.5,                # tous les 0.5%
        segment_distance_range=(0.5, 100),     # km (distance totale du segment)
        segment_distance_step=0.5,             # tous les 500m
        mass_total=75,                         # kg (cycliste + vélo)
        verbose=True
    ):
        """
        Construit la lookup table 3D: (segment_distance, section_distance, section_grade) → time(s)
        
        La table contient le TEMPS EN SECONDES pour parcourir une section donnée,
        en fonction de:
        - La distance totale du segment (détermine la puissance via power profile)
        - La distance de la section
        - Le grade de la section
        
        Args:
            section_distance_range: (min, max) distance des sections en km
            section_distance_step: pas de discrétisation des distances
            section_grade_range: (min, max) grade en %
            section_grade_step: pas de discrétisation des grades
            segment_distance_range: (min, max) distance totale du segment en km
            segment_distance_step: pas de discrétisation
            mass_total: masse totale (cycliste + vélo) en kg
            verbose: afficher la progression
        
        Returns:
            dict avec 'table', 'axes', 'interpolator', 'metadata'
        """
        
        # Créer les axes
        segment_distances = np.arange(
            segment_distance_range[0], 
            segment_distance_range[1] + segment_distance_step, 
            segment_distance_step
        )
        section_distances = np.arange(
            section_distance_range[0], 
            section_distance_range[1] + section_distance_step, 
            section_distance_step
        )
        section_grades = np.arange(
            section_grade_range[0], 
            section_grade_range[1] + section_grade_step, 
            section_grade_step
        )
        
        if verbose:
            print(f"Building 3D lookup table...")
            print(f"  Segment distances: {len(segment_distances)} points ({segment_distance_range[0]}-{segment_distance_range[1]} km)")
            print(f"  Section distances: {len(section_distances)} points ({section_distance_range[0]}-{section_distance_range[1]} km)")
            print(f"  Section grades: {len(section_grades)} points ({section_grade_range[0]}-{section_grade_range[1]} %)")
            print(f"  Total size: {len(segment_distances) * len(section_distances) * len(section_grades):,} entries")
            print(f"  Memory estimate: ~{len(segment_distances) * len(section_distances) * len(section_grades) * 8 / 1e6:.1f} MB")
        
        # Initialiser la table 3D: [segment_dist, section_dist, section_grade]
        lookup_table = np.zeros((len(segment_distances), len(section_distances), len(section_grades)))
        
        # Pré-calculer les durées moyennes des segments (pour estimer la puissance)
        # On estime une vitesse moyenne de 8 m/s (28.8 km/h)
        segment_durations = segment_distances * 1000 / 8  # secondes (estimation)
        
        # Pré-calculer les modèles de vitesse pour chaque durée de segment
        if verbose:
            print("  Pre-computing power models...")
        
        power_models = {}
        for seg_dist, seg_duration in zip(segment_distances, segment_durations):
            power = self.power_profile(seg_duration)
            if power not in power_models:
                power_models[power] = self.build_power_model(power=power, mass_total=mass_total)
        
        # Remplir la table
        if verbose:
            print("  Filling lookup table...")
        
        total_iterations = len(segment_distances)
        for i, (seg_dist, seg_duration) in enumerate(zip(segment_distances, segment_durations)):
            if verbose and i % 20 == 0:
                print(f"    Progress: {i}/{total_iterations} ({i/total_iterations*100:.1f}%)")
            
            # Puissance disponible pour ce segment
            power = self.power_profile(seg_duration)
            velocity_model = power_models[power]
            
            for j, sect_dist in enumerate(section_distances):
                for k, grade in enumerate(section_grades):
                    # Calculer la vitesse pour ce grade
                    velocity = velocity_model(grade)  # m/s
                    
                    # Temps pour parcourir la section
                    time_seconds = (sect_dist * 1000) / velocity  # distance(m) / vitesse(m/s)
                    
                    # Stocker dans la table
                    lookup_table[i, j, k] = time_seconds
        
        if verbose:
            print("✓ Lookup table built successfully!")
        
        # Créer l'interpolateur 3D
        interpolator = RegularGridInterpolator(
            (segment_distances, section_distances, section_grades),
            lookup_table,
            method='linear',
            bounds_error=False,
            fill_value=None  # Extrapolation si nécessaire
        )
        
        result = {
            'table': lookup_table,
            'axes': {
                'segment_distances': segment_distances,
                'section_distances': section_distances,
                'section_grades': section_grades
            },
            'interpolator': interpolator,
            'metadata': {
                'mass_total': mass_total,
                'unit': 'seconds',
                'description': 'Time to complete section (seconds)'
            }
        }
        
        return result


    # ============================================================================
    # SAUVEGARDE / CHARGEMENT
    # ============================================================================

    def save_lookup_table(self, lookup_dict, filename='ride_lookup_table.pkl'):
        """Sauvegarde la lookup table sur disque"""
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            pickle.dump(lookup_dict, f, protocol=pickle.HIGHEST_PROTOCOL)
        
        size_mb = len(pickle.dumps(lookup_dict)) / 1e6
        print(f"✓ Lookup table saved to {path} ({size_mb:.1f} MB)")


    def load_lookup_table(self, filename='ride_lookup_table.pkl'):
        """Charge la lookup table depuis le disque"""
        path = Path(filename)
        with open(path, 'rb') as f:
            lookup_dict = pickle.load(f)
        print(f"✓ Lookup table loaded from {path}")
        return lookup_dict


    # ============================================================================
    # UTILISATION RAPIDE
    # ============================================================================

    def compute_segment_time_fast(self, sections, segment_distance_km, lookup_dict):
        """
        Calcule le temps total RAPIDEMENT en utilisant la lookup table.
        
        Args:
            sections: Liste de dict avec 'distance' (m) et 'grade' (%)
            segment_distance_km: Distance totale du segment en km
            lookup_dict: Dictionnaire retourné par build_lookup_table_3d()
        
        Returns:
            float: Temps total en secondes
        """
        interpolator = lookup_dict['interpolator']
        
        total_time = 0.0
        
        for section in sections:
            sect_dist_km = section['distance'] / 1000  # convertir en km
            grade = section['grade']
            
            # Lookup dans la table 3D: (segment_dist, section_dist, grade) → time(s)
            time_seconds = interpolator([segment_distance_km, sect_dist_km, grade])[0]
            
            total_time += time_seconds
        
        return total_time


    # ============================================================================
    # EXEMPLE D'UTILISATION
    # ============================================================================

if __name__ == "__main__":
    # 1. Construire la table (à faire UNE SEULE FOIS)
    print("\n" + "="*70)
    print("BUILDING LOOKUP TABLE")
    print("="*70 + "\n")
    generator = RideLookUpTableGenerator()
    
    lookup_dict = generator.build_lookup_table_3d(
        section_distance_range=(0.05, 10.0),
        section_distance_step=0.05,
        section_grade_range=(-10, 20),
        section_grade_step=0.5,
        segment_distance_range=(0.5, 100),
        segment_distance_step=0.5,
        mass_total=75,
        verbose=True
    )
    
    # 2. Sauvegarder
    a_str = str(generator.a).replace('.', '')
    file_name = f'{generator.P_inf}W_{generator.AWC}Ws_{a_str}_power_time_LUT_ride.pkl'
    dest_path = repo_root / 'src' / 'lookup_tables' / file_name
    generator.save_lookup_table(lookup_dict, dest_path)