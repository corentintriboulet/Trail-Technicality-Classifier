import numpy as np
import pickle
from scipy.optimize import fsolve
from scipy.interpolate import interp1d, RegularGridInterpolator
import sys
from pathlib import Path
current_script_path = Path(__file__).resolve()
repo_root = current_script_path.parents[2] 
sys.path.append(str(repo_root))

class RunLookUpTableGenerator:
    
    # ============================================================================
    # RUNNER PROFILE
    # ============================================================================
    VO2max = 60  # ml/kg/min
    vVO2max = 6  # m/s 

    # ============================================================================
    # RIGEL FORMULA - Équivalent de la Power Curve
    # ============================================================================

    def riegel_formula(self, distance_m, vVO2max=None):
        tps_VO2max_s = 6*60
        a=1.06
        return tps_VO2max_s*(distance_m/(tps_VO2max_s*vVO2max))**(a)

    def speed_profile_riegel(self, duration_s):
        """
        Profil de vitesse en fonction de la durée de l'effort
        Équivalent de power_profile() pour la course
        
        Args:
            duration_s: durée de l'effort en secondes
        
        Returns:
            vitesse de base en m/s
        """
        a = 1.06
        tps_VO2max_s = 6*60  # temps maximal à VO2max en secondes (6 min)
        return self.vVO2max * (duration_s / tps_VO2max_s) ** ((1-a) / a)

    def speed_profile(self, duration_s, vVO2max=None):
        """
        Profil de vitesse en fonction de la durée de l'effort
        Utilise une formule empirique basée sur les performances
        Args:
            duration_s: durée de l'effort en secondes
        Returns:
            vitesse de base en m/s
        """
        a = 0.05
        return vVO2max/10 * 10**((duration_s/(6*60))**(-a))

    def vo2_utilization(self, duration_s):
        """
        Pourcentage de VO2max utilisable en fonction de la durée
        
        Args:
            duration_s: durée en secondes
        
        Returns:
            fraction de VO2max (0-1)
        """

        return 0.8 + 0.18 * np.exp(-0.014*duration_s)


    # ============================================================================
    # MINETTI COST - Coût énergétique
    # ============================================================================

    def minetti_cost(self, grade_percent):
        """
        Coût énergétique en J/(kg·m) selon Minetti et al. (2002)
        
        Args:
            grade_percent: pente en %
        
        Returns:
            coût énergétique en J/(kg·m)
        """
        i = grade_percent / 100  # conversion en tangente (approximation)
        
        cost = (155.4 * i**5 - 
                30.4 * i**4 - 
                43.3 * i**3 + 
                46.3 * i**2 + 
                19.5 * i + 
                3.6)
        
        return max(cost, 1.0)  # minimum physiologique

    def walking_speed(self, grade_percent):
        """
        Vitesse de marche en montée raide
        
        Args:
            grade_percent: pente en %
        
        Returns:
            vitesse en m/s
        """
        # Modèle empirique : vitesse décroît avec la pente
        if grade_percent < 15:
            return 1.4  # marche rapide
        elif grade_percent < 25:
            return 1.4 - 0.4 * (grade_percent - 15) / 10
        else:
            return max(0.8, 1.0 - 0.02 * (grade_percent - 25))


    # ============================================================================
    # MODÈLE PHYSIQUE - Runner Profile → Vitesse sur section
    # ============================================================================

    def build_velocity_model(self, segment_distance_km, runner_vo2max=None, runner_vVO2max=None):
        """
        Construit un modèle qui convertit grade → vitesse pour un segment donné
        Équivalent de build_power_model() pour la course
        
        Args:
            segment_distance_km: distance totale du segment en km
            runner_vo2max: VO2max du coureur en ml/kg/min
            runner_mass: masse du coureur en kg
        
        Returns:
            function: interpolateur grade(%) → vitesse(m/s)
        """
        # Estimer la durée du segment avec Riegel
        estimated_duration = self.riegel_formula(segment_distance_km * 1000, vVO2max=runner_vVO2max)
        
        # Vitesse de base pour ce segment
        v_base = segment_distance_km/(estimated_duration / 3600)  # m/s
        vo2_percent = self.vo2_utilization(estimated_duration)
        
        # Générer la courbe grade → vitesse
        grades = np.linspace(-30, 40, 701)  # range plus large qu'en vélo
        velocities = []
        
        cost_flat = self.minetti_cost(0)
        vo2_available = runner_vo2max * vo2_percent  # ml/kg/min
        
        for grade in grades:
            cost_grade = self.minetti_cost(grade)
            
            # Vitesse ajustée selon le coût énergétique
            # Principe : si coût augmente, vitesse diminue
            v_adjusted = v_base * np.sqrt(cost_flat / cost_grade)
            
            # Vérifier si on dépasse VO2max
            # VO2 requis (ml/kg/min) = cost (J/kg/m) * velocity (m/s) * 60 / 4.184
            vo2_required = (cost_grade * v_adjusted * 60) / 4.184  # conversion approximative
            
            if vo2_required > vo2_available:
                # Limiter la vitesse pour rester sous VO2max
                v_adjusted = (vo2_available * 4.184) / (cost_grade * 60)
            
            # Limite marche en montée raide
            if grade > 15 and v_adjusted < 1.5:
                v_adjusted = self.walking_speed(grade)
            
            # Limite descente (freinage)
            if grade < -15:
                max_descent_speed = v_base * 1.3  # max +30% en descente
                v_adjusted = min(v_adjusted, max_descent_speed)
            
            velocities.append(max(v_adjusted, 0.5))  # vitesse minimale
        
        # Créer l'interpolateur
        from scipy.interpolate import interp1d
        interpolator = interp1d(
            grades,
            velocities,
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
        section_distance_range=(0.05, 5.0),    # km (sections plus courtes en trail)
        section_distance_step=0.05,             # tous les 50m
        section_grade_range=(-30, 40),          # % (range plus large)
        section_grade_step=1.0,                 # tous les 1%
        segment_distance_range=(1.0, 100),      # km (1km à ultra)
        segment_distance_step=0.5,              # tous les 500m
        runner_vo2max=VO2max,
        verbose=True
    ):
        """
        Construit la lookup table 3D pour la course à pied
        (segment_distance, section_distance, section_grade) → time(s)
        
        Args:
            section_distance_range: (min, max) distance des sections en km
            section_distance_step: pas de discrétisation
            section_grade_range: (min, max) grade en %
            section_grade_step: pas de discrétisation
            segment_distance_range: (min, max) distance totale du segment en km
            segment_distance_step: pas de discrétisation
            runner_vo2max: VO2max en ml/kg/min
            runner_mass: masse en kg
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
            print(f"Building 3D lookup table for TRAIL RUNNING...")
            print(f"  Segment distances: {len(segment_distances)} points ({segment_distance_range[0]}-{segment_distance_range[1]} km)")
            print(f"  Section distances: {len(section_distances)} points ({section_distance_range[0]}-{section_distance_range[1]} km)")
            print(f"  Section grades: {len(section_grades)} points ({section_grade_range[0]}-{section_grade_range[1]} %)")
            print(f"  Total size: {len(segment_distances) * len(section_distances) * len(section_grades):,} entries")
            print(f"  Memory estimate: ~{len(segment_distances) * len(section_distances) * len(section_grades) * 8 / 1e6:.1f} MB")
        
        # Initialiser la table 3D
        lookup_table = np.zeros((len(segment_distances), len(section_distances), len(section_grades)))
        
        # Pré-calculer les modèles de vitesse pour chaque distance de segment
        if verbose:
            print("  Pre-computing velocity models...")
        
        velocity_models = {}
        for seg_dist in segment_distances:
            velocity_models[seg_dist] = self.build_velocity_model(
                seg_dist, 
                runner_vo2max=runner_vo2max,
                runner_vVO2max=self.vVO2max
            )
        
        # Remplir la table
        if verbose:
            print("  Filling lookup table...")
        
        total_iterations = len(segment_distances)
        for i, seg_dist in enumerate(segment_distances):
            if verbose and i % 20 == 0:
                print(f"    Progress: {i}/{total_iterations} ({i/total_iterations*100:.1f}%)")
            
            velocity_model = velocity_models[seg_dist]
            
            for j, sect_dist in enumerate(section_distances):
                for k, grade in enumerate(section_grades):
                    # Calculer la vitesse pour ce grade
                    velocity = velocity_model(grade)  # m/s
                    
                    # Temps pour parcourir la section
                    time_seconds = (sect_dist * 1000) / velocity
                    
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
            fill_value=None
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
                'runner_vo2max': runner_vo2max,
                'runner_vV02max': self.vVO2max,
                'unit': 'seconds',
                'description': 'Time to complete section (seconds) - Trail Running Model'
            }
        }
        
        return result


    # ============================================================================
    # SAUVEGARDE / CHARGEMENT
    # ============================================================================

    def save_lookup_table(self, lookup_dict, filename='trail_running_lookup_table.pkl'):
        """Sauvegarde la lookup table sur disque"""
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            pickle.dump(lookup_dict, f, protocol=pickle.HIGHEST_PROTOCOL)
        
        size_mb = len(pickle.dumps(lookup_dict)) / 1e6
        print(f"✓ Lookup table saved to {path} ({size_mb:.1f} MB)")


    def load_lookup_table(self, filename='trail_running_lookup_table.pkl'):
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
        Calcule le temps total en utilisant la lookup table
        
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
            sect_dist_km = section['distance'] / 1000
            grade = section['grade']
            
            # Lookup dans la table 3D
            time_seconds = interpolator([segment_distance_km, sect_dist_km, grade])[0]
            
            total_time += time_seconds
        
        return total_time

if __name__ == "__main__":
    print("\n" + "="*70)
    print("BUILDING LOOKUP TABLE")
    print("="*70 + "\n")
    generator = RunLookUpTableGenerator()
    lookup_dict = generator.build_lookup_table_3d(
        section_distance_range=(0.05, 10.0),
        section_distance_step=0.05,
        section_grade_range=(-10, 20),
        section_grade_step=0.5,
        segment_distance_range=(0.5, 100),
        segment_distance_step=0.5,
        verbose=True
    )
    
    # 2. Sauvegarder

    file_name = f'{generator.VO2max}ml_kg_min_{generator.vVO2max}m_s_distance_time_LUT_run.pkl'
    destination_path = repo_root / 'src' / 'lookup_tables' / file_name
    generator.save_lookup_table(lookup_dict, destination_path)