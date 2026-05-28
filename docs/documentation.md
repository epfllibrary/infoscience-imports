# Guide d'utilisation — Infoscience Import Pipeline

Ce fichier est un placeholder. Remplacez son contenu par la documentation locale complète.

La page **Aide** de l'UI charge ce fichier au démarrage. Pour enrichir la documentation,
copiez votre contenu ici et redémarrez l'UI — aucun redémarrage du serveur n'est nécessaire,
le fichier est lu à chaque chargement de page.

Une fois remplacé, exécutez la commande suivante pour que git ne traque plus vos modifications locales :

```bash
git update-index --skip-worktree docs/documentation.md
```

Pour vérifier l'état :

```bash
git ls-files -v docs/documentation.md   # affiche 'S' si skip-worktree est actif
```

Pour réactiver le tracking (ex. pour mettre à jour le placeholder) :

```bash
git update-index --no-skip-worktree docs/documentation.md
```
