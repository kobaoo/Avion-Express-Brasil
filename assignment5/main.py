import open3d as o3d
import numpy as np

MODEL_PATH = "T_ronda5a.obj"

NUM_SAMPLED_POINTS = 35000
VOXEL_SIZE = 0.05
POISSON_DEPTH = 6


def step1_load_mesh(path: str) -> o3d.geometry.TriangleMesh:
    mesh = o3d.io.read_triangle_mesh(path)
    if not mesh.has_vertex_normals():
        mesh.compute_vertex_normals()

    print("STEP 1: ORIGINAL MESH")
    print("  vertices:", np.asarray(mesh.vertices).shape[0])
    print("  triangles:", np.asarray(mesh.triangles).shape[0])
    print("  has vertex colors:", mesh.has_vertex_colors())
    print("  has vertex normals:", mesh.has_vertex_normals())

    o3d.visualization.draw_geometries([mesh], window_name="1. Original Mesh")
    return mesh


def step2_mesh_to_clean_point_cloud(mesh: o3d.geometry.TriangleMesh,
                                    n_points: int) -> o3d.geometry.PointCloud:
    """сэмплируем точки и обрезаем верхние 'пузырьки'"""
    pcd = mesh.sample_points_uniformly(number_of_points=n_points)
    print("\nSTEP 2: POINT CLOUD (raw)")
    print("  points:", len(pcd.points))

    # получаем bbox и обрезаем верхние ~35% по оси Z (вертикаль)
    min_b = pcd.get_min_bound()
    max_b = pcd.get_max_bound()

    # Обрезаем по Z (вертикальная ось)
    max_b[2] = min_b[2] + (max_b[2] - min_b[2]) * 1

    cleaned = pcd.crop(o3d.geometry.AxisAlignedBoundingBox(min_b, max_b))

    print("  points after cleaning:", len(cleaned.points))

    o3d.visualization.draw_geometries([cleaned], window_name="2. Cleaned Point Cloud")
    return cleaned


def step3_poisson_and_crop(pcd: o3d.geometry.PointCloud) -> o3d.geometry.TriangleMesh:
    """восстановление поверхности и обрезка по bbox"""
    mesh_rec, densities = o3d.geometry.TriangleMesh.create_from_point_cloud_poisson(
        pcd, depth=POISSON_DEPTH
    )

    print("\nSTEP 3: POISSON RECONSTRUCTION (raw)")
    print("  vertices:", len(mesh_rec.vertices))
    print("  triangles:", len(mesh_rec.triangles))

    # обрезаем по bbox исходного pcd, чтобы убрать лишнее
    bbox = pcd.get_axis_aligned_bounding_box()
    bbox = bbox.scale(1.02, bbox.get_center())
    mesh_rec = mesh_rec.crop(bbox)
    mesh_rec.compute_vertex_normals()

    print("  after crop -> vertices:", len(mesh_rec.vertices))
    print("  after crop -> triangles:", len(mesh_rec.triangles))

    o3d.visualization.draw_geometries([mesh_rec], window_name="3. Reconstructed Mesh (Poisson)")
    return mesh_rec


def step4_voxelize(mesh: o3d.geometry.TriangleMesh,
                   voxel_size: float) -> o3d.geometry.VoxelGrid:
    voxel_grid = o3d.geometry.VoxelGrid.create_from_triangle_mesh(mesh, voxel_size)
    print("\nSTEP 4: VOXELIZATION")
    print("  voxel size:", voxel_size)
    print("  num voxels:", len(voxel_grid.get_voxels()))
    o3d.visualization.draw_geometries([voxel_grid], window_name="4. Voxelized Model")
    return voxel_grid


def step5_create_plane_near_mesh(mesh: o3d.geometry.TriangleMesh) -> o3d.geometry.TriangleMesh:
    """Создаем плоскость для обрезки - горизонтальную плоскость"""
    # Получаем bounding box меша
    bbox = mesh.get_axis_aligned_bounding_box()
    center = bbox.get_center()
    min_bound = bbox.get_min_bound()
    max_bound = bbox.get_max_bound()
    
    # Размеры плоскости делаем немного больше модели
    width = (max_bound[0] - min_bound[0]) * 1.5
    depth = (max_bound[1] - min_bound[1]) * 1.5
    
    # Создаем плоскость
    plane = o3d.geometry.TriangleMesh.create_box(width=width, height=0.01, depth=depth)
    plane.paint_uniform_color([0.3, 0.3, 0.3])

    # Центрируем плоскость по X и Y, размещаем ПОД моделью по Z
    plane_center = plane.get_center()
    plane.translate([
        center[0] - plane_center[0],  # Центрируем по X
        center[1] - plane_center[1],  # Центрируем по Y  
        min_bound[2] - 0.05           # Размещаем НИЖЕ модели (было -0.1)
    ])

    print("\nSTEP 5: PLANE + MESH")
    print(f"  Mesh bbox min: {min_bound}")
    print(f"  Mesh bbox max: {max_bound}")
    print(f"  Mesh center: {center}")
    print(f"  Plane center: {plane.get_center()}")
    print(f"  Plane size: {width:.2f} x {depth:.2f}")
    
    o3d.visualization.draw_geometries([plane, mesh], window_name="5. Plane + Mesh")
    return plane


def step6_clip_by_plane(pcd: o3d.geometry.PointCloud,
                        plane: o3d.geometry.TriangleMesh) -> o3d.geometry.PointCloud:
    plane_point = plane.get_center()
    plane_normal = np.array([0.0, 1.0, 0.0])   # ось Y

    points = np.asarray(pcd.points)
    colors = np.asarray(pcd.colors) if pcd.has_colors() else None

    vec = points - plane_point
    dot = vec @ plane_normal

    mask = dot < 0   # оставляем точки ниже плоскости

    clipped = o3d.geometry.PointCloud()
    clipped.points = o3d.utility.Vector3dVector(points[mask])
    if colors is not None and colors.shape[0] == points.shape[0]:
        clipped.colors = o3d.utility.Vector3dVector(colors[mask])

    print("\nSTEP 6: CLIPPED POINT CLOUD")
    print("  points after clipping:", len(clipped.points))

    o3d.visualization.draw_geometries([clipped], window_name="6. Clipped Point Cloud")
    return clipped

def step7_color_and_mark_extremes(pcd: o3d.geometry.PointCloud):
    """Раскрашиваем по высоте (Z) и отмечаем экстремальные точки"""
    if len(pcd.points) == 0:
        print("ERROR: No points in point cloud!")
        return

    pts = np.asarray(pcd.points)
    z = pts[:, 2]  # Используем ось Z для высоты
    z_min, z_max = z.min(), z.max()
    z_norm = (z - z_min) / (z_max - z_min + 1e-8)

    colors = np.zeros((pts.shape[0], 3))
    colors[:, 0] = z_norm  # Красный увеличивается с высотой
    colors[:, 2] = 1 - z_norm  # Синий уменьшается с высотой
    pcd.colors = o3d.utility.Vector3dVector(colors)

    idx_min = int(np.argmin(z))
    idx_max = int(np.argmax(z))
    p_min = pts[idx_min]
    p_max = pts[idx_max]

    # Создаем маркеры для экстремальных точек
    min_sphere = o3d.geometry.TriangleMesh.create_sphere(radius=0.05)  # Уменьшил радиус
    min_sphere.translate(p_min)
    min_sphere.paint_uniform_color([0, 1, 0])  # Зеленый для минимума

    max_sphere = o3d.geometry.TriangleMesh.create_sphere(radius=0.05)  # Уменьшил радиус
    max_sphere.translate(p_max)
    max_sphere.paint_uniform_color([1, 0, 0])  # Красный для максимума

    print("\nSTEP 7: COLOR + EXTREMES")
    print("  Z min point:", p_min)
    print("  Z max point:", p_max)
    print("  Height range:", z_max - z_min)

    o3d.visualization.draw_geometries(
        [pcd, min_sphere, max_sphere],
        window_name="7. Colored by Z + Extremes"
    )


def main():
    mesh = step1_load_mesh(MODEL_PATH)
    pcd = step2_mesh_to_clean_point_cloud(mesh, NUM_SAMPLED_POINTS)
    mesh_rec = step3_poisson_and_crop(pcd)
    _ = step4_voxelize(mesh_rec, VOXEL_SIZE)
    plane = step5_create_plane_near_mesh(mesh_rec)
    clipped_pcd = step6_clip_by_plane(pcd, plane)
    step7_color_and_mark_extremes(clipped_pcd)


if __name__ == "__main__":
    main()